use std::collections::BTreeMap;
use std::{fmt::Debug, path::PathBuf, sync::Arc};

use crate::bus::{BusClientSender, SharedMessageBus};
use crate::{log_error, module_bus_client, module_handle_messages, modules::Module};
use anyhow::{anyhow, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::rest_client::NodeApiClient;
use client_sdk::{helpers::ClientSdkProver, transaction_builder::TxExecutorHandler};
use hyle_net::logged_task::logged_task;
use indexmap::IndexMap;
use sdk::{
    BlobIndex, BlobTransaction, Block, BlockHeight, Calldata, ContractName, Hashed, NodeStateEvent,
    ProofTransaction, TransactionData, TxContext, TxHash, TxId, HYLE_TESTNET_CHAIN_ID,
};
use tracing::{debug, error, info, trace, warn};

use super::prover_metrics::AutoProverMetrics;

/// `AutoProver` is a module that handles the proving of transactions
/// It listens to the node state events and processes all blobs in the block's transactions
/// for a given contract.
/// It asynchronously generates 1 ProofTransaction to prove all concerned blobs in a block
/// If a passed BlobTransaction times out, or is settled as failed, all blobs that are "after"
/// the failed transaction in the block are re-executed, and prooved all at once, even if in
/// multiple blocks.
/// This module requires the ELF to support multiproof. i.e. it requires the ELF to read
/// a `Vec<Calldata>` as input.
pub struct AutoProver<Contract: Send + Sync + Clone + 'static> {
    bus: AutoProverBusClient<Contract>,
    ctx: Arc<AutoProverCtx<Contract>>,
    store: AutoProverStore<Contract>,
    metrics: AutoProverMetrics,
    // If Some, the block to catch up to
    catching_up: Option<BlockHeight>,

    catching_txs: IndexMap<TxId, (BlobTransaction, TxContext)>,
    catching_success_txs: Vec<(BlobTransaction, TxContext)>,
}

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct AutoProverStore<Contract> {
    unsettled_txs: Vec<(BlobTransaction, TxContext)>,
    state_history: BTreeMap<TxHash, (Contract, bool)>,
    tx_chain: Vec<TxHash>,
    buffered_blobs: Vec<(BlobIndex, BlobTransaction, TxContext)>,
    buffered_blocks_count: u32,
    batch_id: u64,
}

module_bus_client! {
#[derive(Debug)]
pub struct AutoProverBusClient<Contract: Send + Sync + Clone + 'static> {
    sender(AutoProverEvent<Contract>),
    receiver(NodeStateEvent),
}
}

pub struct AutoProverCtx<Contract> {
    pub data_directory: PathBuf,
    pub prover: Arc<dyn ClientSdkProver<Vec<Calldata>> + Send + Sync>,
    pub contract_name: ContractName,
    pub node: Arc<dyn NodeApiClient + Send + Sync>,
    pub default_state: Contract,
    /// How many blocks should we buffer before generating proofs ?
    pub buffer_blocks: u32,
    pub max_txs_per_proof: usize,
}

#[derive(Debug, Clone)]
pub enum AutoProverEvent<Contract> {
    /// Event sent when a blob is executed as failed
    /// proof will be generated & sent to the node
    FailedTx(TxHash, String),
    /// Event sent when a blob is executed as success
    /// proof will be generated & sent to the node
    SuccessTx(TxHash, Contract),
}

impl<Contract> Module for AutoProver<Contract>
where
    Contract: TxExecutorHandler
        + BorshSerialize
        + BorshDeserialize
        + Debug
        + Send
        + Sync
        + Clone
        + 'static,
{
    type Context = Arc<AutoProverCtx<Contract>>;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = AutoProverBusClient::<Contract>::new_from_bus(bus.new_handle()).await;

        let file = ctx
            .data_directory
            .join(format!("autoprover_{}.bin", ctx.contract_name).as_str());

        let store = match Self::load_from_disk::<AutoProverStore<Contract>>(file.as_path()) {
            Some(store) => store,
            None => AutoProverStore::<Contract> {
                unsettled_txs: vec![],
                state_history: BTreeMap::new(),
                tx_chain: vec![],
                buffered_blobs: vec![],
                buffered_blocks_count: 0,
                batch_id: 0,
            },
        };

        let infos = ctx.prover.info();

        let metrics = AutoProverMetrics::global(ctx.contract_name.to_string(), infos);

        let current_block = ctx.node.get_block_height().await?;
        let catching_up = match current_block.0 > 0 {
            true => Some(current_block),
            false => None,
        };

        info!(
            cn =% ctx.contract_name,
            "Catching up to {:?}",
            catching_up
        );

        Ok(AutoProver {
            bus,
            store,
            ctx,
            metrics,
            catching_up,
            catching_success_txs: vec![],
            catching_txs: IndexMap::new(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_bus self.bus,
            listen<NodeStateEvent> event => {
                _ = log_error!(self.handle_node_state_event(event).await, "handle note state event");
                self.metrics.snapshot_buffered_blobs(self.store.buffered_blobs.len() as u64);
                self.metrics
                    .snapshot_unsettled_blobs(self.store.unsettled_txs.len() as u64);
            }
        };

        let _ = log_error!(
            Self::save_on_disk::<AutoProverStore<Contract>>(
                self.ctx
                    .data_directory
                    .join(format!("autoprover_{}.bin", self.ctx.contract_name))
                    .as_path(),
                &self.store,
            ),
            "Saving prover"
        );

        Ok(())
    }
}

impl<Contract> AutoProver<Contract>
where
    Contract: TxExecutorHandler + Debug + Clone + Send + Sync + 'static,
{
    async fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<()> {
        let NodeStateEvent::NewBlock(block) = event;
        if self
            .catching_up
            .is_some_and(|h| block.block_height.0 <= h.0)
        {
            let block_height = block.block_height;
            self.handle_catchup_block(*block)
                .await
                .context("Failed to handle settled block")?;
            if self.catching_up.is_some_and(|h| block_height.0 == h.0) {
                // Build blobs to execute from catching_txs
                let mut blobs: Vec<(BlobIndex, BlobTransaction, TxContext)> = vec![];
                for (tx, tx_ctx) in self.catching_success_txs.iter() {
                    for (index, blob) in tx.blobs.iter().enumerate() {
                        if blob.contract_name == self.ctx.contract_name {
                            blobs.push((index.into(), tx.clone(), tx_ctx.clone()));
                        }
                    }
                }

                info!(
                    cn =% self.ctx.contract_name,
                    "✅ Catching up finished, {} blobs to process",
                    blobs.len()
                );
                // Clear our flag.
                self.catching_up = None;

                let mut contract = self.ctx.default_state.clone();
                let Some(last_tx_hash) = blobs.last().map(|(_, tx, _)| tx.hashed()) else {
                    return Ok(());
                };
                for (blob_index, tx, tx_ctx) in blobs {
                    let calldata = Calldata {
                        identity: tx.identity.clone(),
                        tx_hash: tx.hashed(),
                        private_input: vec![],
                        blobs: tx.blobs.clone().into(),
                        index: blob_index,
                        tx_ctx: Some(tx_ctx.clone()),
                        tx_blob_count: tx.blobs.len(),
                    };

                    match contract.handle(&calldata) {
                        Err(e) => {
                            error!(
                                cn =% self.ctx.contract_name,
                                tx_hash =% tx.hashed(),
                                tx_height =% tx_ctx.block_height,
                                "Error while executing settled tx: {e:#}"
                            );
                            error!(
                                cn =% self.ctx.contract_name,
                                tx_hash =% tx.hashed(),
                                tx_height =% tx_ctx.block_height,
                                "This is likely a bug in the prover, please report it to the Hyle team."
                            );
                        }
                        Ok(hyle_output) => {
                            info!(
                                cn =% self.ctx.contract_name,
                                tx_hash =% tx.hashed(),
                                tx_height =% tx_ctx.block_height,
                                "Executed contract: {}. Success: {}",
                                String::from_utf8_lossy(&hyle_output.program_outputs),
                                hyle_output.success
                            );
                            if !hyle_output.success {
                                error!(
                                    cn =% self.ctx.contract_name,
                                    tx_hash =% tx.hashed(),
                                    tx_height =% tx_ctx.block_height,
                                    "Executed tx as failed but it was settled as success!",
                                );
                                error!(
                                    cn =% self.ctx.contract_name,
                                    tx_hash =% tx.hashed(),
                                    tx_height =% tx_ctx.block_height,
                                    "This is likely a bug in the prover, please report it to the Hyle team."
                                );
                            }
                        }
                    }
                }
                info!(
                    cn =% self.ctx.contract_name,
                    "All catching blobs processed, catching up finished at block {} with tx {}",
                    block_height,
                    last_tx_hash
                );

                #[cfg(not(test))]
                {
                    let final_state = contract.get_state_commitment();
                    info!(
                        cn =% self.ctx.contract_name,
                        "Final state after catching up: {:?}",
                        final_state
                    );
                    let onchain = self
                        .ctx
                        .node
                        .get_contract(self.ctx.contract_name.clone())
                        .await?;

                    if onchain.state != final_state {
                        error!(
                            cn =% self.ctx.contract_name,
                            "Onchain state does not match final state after catching up. Onchain: {:?}, Final: {:?}",
                            onchain, final_state
                        );
                        error!(
                            cn =% self.ctx.contract_name,
                            "This is likely a bug in the prover, please report it to the Hyle team."
                        );
                        anyhow::bail!(
                        "Onchain state does not match final state after catching up. Onchain: {:?}, Final: {:?}",
                        onchain, final_state
                    );
                    }
                }

                self.store.tx_chain = vec![last_tx_hash.clone()];
                self.store.state_history.insert(last_tx_hash, contract);

                // Now any remaining TX is to be buffered and handled on the next block
                info!(
                    cn =% self.ctx.contract_name,
                    "Buffering remaining {} unsettled TXs after catching up",
                    self.catching_txs.len()
                );
                for (tx, tx_ctx) in std::mem::take(&mut self.catching_txs).into_values() {
                    self.store.tx_chain.push(tx.hashed());
                    let blobs = self.handle_blob(tx, tx_ctx);
                    self.store.buffered_blobs.extend_from_slice(&blobs);
                }
            }
        } else {
            self.handle_processed_block(*block).await?;
        }

        Ok(())
    }

    async fn handle_catchup_block(&mut self, block: Block) -> Result<()> {
        for (_, tx) in block.txs {
            if let TransactionData::Blob(tx) = tx.transaction_data {
                if tx
                    .blobs
                    .iter()
                    .all(|b| b.contract_name != self.ctx.contract_name)
                {
                    continue;
                }
                let tx_ctx = TxContext {
                    block_height: block.block_height,
                    block_hash: block.hash.clone(),
                    timestamp: block.block_timestamp.clone(),
                    lane_id: block
                        .lane_ids
                        .get(&tx.hashed())
                        .ok_or_else(|| anyhow!("Missing lane id in block for {}", tx.hashed()))?
                        .clone(),
                    chain_id: HYLE_TESTNET_CHAIN_ID,
                };
                self.catching_txs.insert(
                    TxId(
                        block
                            .dp_parent_hashes
                            .get(&tx.hashed())
                            .ok_or_else(|| anyhow!("Missing DP in block for {}", tx.hashed()))?
                            .clone(),
                        tx.hashed(),
                    ),
                    (tx.clone(), tx_ctx),
                );
            }
        }

        // Only used to reduce size of catching_txs
        for tx in block.timed_out_txs {
            let tx_id = TxId(
                block
                    .dp_parent_hashes
                    .get(&tx)
                    .ok_or_else(|| anyhow!("Missing DP in block for {}", tx))?
                    .clone(),
                tx,
            );

            self.catching_txs.retain(|t, _| t != &tx_id);
        }

        // Only used to reduce size of catching_txs
        for tx in block.failed_txs {
            let tx_id = TxId(
                block
                    .dp_parent_hashes
                    .get(&tx)
                    .ok_or_else(|| anyhow!("Missing DP in block for {}", tx))?
                    .clone(),
                tx,
            );

            self.catching_txs.retain(|t, _| t != &tx_id);
        }

        for tx in block.successful_txs {
            let tx_id = TxId(
                block
                    .dp_parent_hashes
                    .get(&tx)
                    .ok_or_else(|| anyhow!("Missing DP in block for {}", tx))?
                    .clone(),
                tx,
            );

            if let Some((tx, tx_ctx)) = self.catching_txs.shift_remove(&tx_id) {
                warn!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx.hashed(),
                    "Settling catching tx {} as success",
                    tx.hashed()
                );
                self.catching_success_txs.push((tx, tx_ctx));
            }
        }

        Ok(())
    }

    async fn handle_processed_block(&mut self, block: Block) -> Result<()> {
        tracing::trace!(
            cn =% self.ctx.contract_name,
            block_height =% block.block_height,
            "Handling processed block {}",
            block.block_height
        );
        let mut blobs = vec![];
        let mut insta_failed_txs = vec![];
        if block.block_height.0 % 1000 == 0 {
            info!(
                cn =% self.ctx.contract_name,
                block_height =% block.block_height,
                "Processing block {}",
                block.block_height
            );
        }

        for (_, tx) in block.txs {
            if let TransactionData::Blob(tx) = tx.transaction_data {
                if tx
                    .blobs
                    .iter()
                    .all(|b| b.contract_name != self.ctx.contract_name)
                {
                    continue;
                }
                if self.store.tx_chain.contains(&tx.hashed()) {
                    debug!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx.hashed(),
                        "🔇 Transaction {} already processed, skipping",
                        tx.hashed()
                    );
                    continue;
                }
                if block.failed_txs.contains(&tx.hashed()) {
                    debug!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx.hashed(),
                        "🔇 Transaction {} insta-failed in block, skipping",
                        tx.hashed()
                    );
                    insta_failed_txs.push(tx.hashed());
                    continue;
                }
                self.store.tx_chain.push(tx.hashed());

                let tx_ctx = TxContext {
                    block_height: block.block_height,
                    block_hash: block.hash.clone(),
                    timestamp: block.block_timestamp.clone(),
                    lane_id: block
                        .lane_ids
                        .get(&tx.hashed())
                        .ok_or_else(|| anyhow!("Missing lane id in block for {}", tx.hashed()))?
                        .clone(),
                    chain_id: HYLE_TESTNET_CHAIN_ID,
                };
                blobs.extend(self.handle_blob(tx, tx_ctx));
            }
        }

        for tx in block.timed_out_txs {
            self.settle_tx_failed(&tx)?;
        }

        for tx in block.failed_txs {
            if insta_failed_txs.contains(&tx) {
                continue;
            }
            self.settle_tx_failed(&tx)?;
        }

        // 🚨 We have to handle successful transactions after the failed ones,
        // as we drop hitory of previous successful transactions when a transaction succeeds,
        // we won't find the parent state of the failed transaction, thus reverting to default state.
        // Covered by test test_auto_prover_tx_failed_after_success_in_same_block
        for tx in block.successful_txs {
            self.settle_tx_success(&tx)?;
        }

        if self.store.buffered_blobs.len() + blobs.len() >= self.ctx.max_txs_per_proof {
            let remaining_count =
                (self.store.buffered_blobs.len() + blobs.len()) % self.ctx.max_txs_per_proof;
            debug!(
                cn =% self.ctx.contract_name,
                "Buffer is full, processing {} blobs. Max: {}, remaining blobs: {}",
                self.store.buffered_blobs.len() + blobs.len(),
                self.ctx.max_txs_per_proof,
                remaining_count
            );
            let remaining_blobs = blobs.split_off(blobs.len() - remaining_count);

            let mut buffered = self.store.buffered_blobs.drain(..).collect::<Vec<_>>();
            self.store.buffered_blocks_count = 0;
            buffered.extend(blobs);
            self.prove_supported_blob(buffered)?;
            self.store.buffered_blobs = remaining_blobs;
        } else if self.store.buffered_blocks_count >= self.ctx.buffer_blocks {
            let mut buffered = self.store.buffered_blobs.drain(..).collect::<Vec<_>>();
            self.store.buffered_blocks_count = 0;
            buffered.extend(blobs);
            if !buffered.is_empty() {
                debug!(
                    cn =% self.ctx.contract_name,
                    "Buffered blocks achieved, processing {} blobs",
                    buffered.len()
                );
            }

            self.prove_supported_blob(buffered)?;
        } else {
            if !blobs.is_empty() {
                debug!(
                    cn =% self.ctx.contract_name,
                    "🔍️ Buffering {} new blobs to {} already buffered, buffered blocks count: {}",
                    blobs.len(),
                    self.store.buffered_blobs.len(),
                    self.store.buffered_blocks_count,
                );
            }
            self.store.buffered_blobs.append(&mut blobs);
            self.store.buffered_blocks_count += 1;

            trace!(
                cn =% self.ctx.contract_name,
                "Buffered {} / {} blobs, {} / {} blocks.",
                self.store.buffered_blobs.len(),
                self.ctx.max_txs_per_proof,
                self.store.buffered_blocks_count,
                self.ctx.buffer_blocks
            );
        }

        Ok(())
    }

    fn handle_blob(
        &mut self,
        tx: BlobTransaction,
        tx_ctx: TxContext,
    ) -> Vec<(BlobIndex, BlobTransaction, TxContext)> {
        let mut blobs = vec![];
        for (index, blob) in tx.blobs.iter().enumerate() {
            if blob.contract_name == self.ctx.contract_name {
                blobs.push((index.into(), tx.clone(), tx_ctx.clone()));
            }
        }
        debug!(
            cn =% self.ctx.contract_name,
            tx_hash =% tx.hashed(),
            "Adding unsettled tx {}",
            tx.hashed()
        );
        self.store.unsettled_txs.push((tx, tx_ctx));
        blobs
    }

    fn settle_tx_success(&mut self, tx: &TxHash) -> Result<()> {
        let prev_tx = self
            .store
            .tx_chain
            .iter()
            .enumerate()
            .find(|(_, h)| *h == tx)
            .and_then(|(i, _)| {
                if i > 0 {
                    self.store.tx_chain.get(i - 1)
                } else {
                    None
                }
            });
        if let Some(prev_tx) = prev_tx {
            debug!(
                cn =% self.ctx.contract_name,
                tx_hash =% tx,
                "🔥 Removing state history for tx {}",
                prev_tx
            );
            self.store.state_history.remove(prev_tx);
        }
        let pos_chain = self.store.tx_chain.iter().position(|h| h == tx);
        if let Some(pos_chain) = pos_chain {
            debug!(
                cn =% self.ctx.contract_name,
                tx_hash =% tx,
                "Settling tx {}. Previous tx: {:?}, Position in chain: {}",
                tx,
                prev_tx,
                pos_chain
            );
            self.store.tx_chain = self.store.tx_chain.split_off(pos_chain);
        }
        self.settle_tx(tx);
        Ok(())
    }

    fn settle_tx_failed(&mut self, tx: &TxHash) -> Result<()> {
        if let Some(pos) = self.settle_tx(tx) {
            debug!(
                cn =% self.ctx.contract_name,
                tx_hash =% tx,
                "🔥 Failed tx, removing state history for tx {}",
               tx
            );
            let found = self.store.state_history.remove(tx);
            self.store.tx_chain.retain(|h| h != tx);
            self.store
                .buffered_blobs
                .retain(|(_, t, _)| t.hashed() != *tx);
            if let Some((_, success)) = found {
                if success {
                    self.handle_all_next_blobs_after_failed(pos)?;
                } else {
                    debug!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx,
                        "🔀 Tx {} already executed as failed, nothing to re-execute",
                        tx
                    );
                }
            } else {
                debug!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx,
                    "🔀 No state history found for tx {}, nothing to re-execute",
                    tx
                );
            }
        }
        Ok(())
    }

    fn settle_tx(&mut self, hash: &TxHash) -> Option<usize> {
        let tx = self
            .store
            .unsettled_txs
            .iter()
            .position(|(t, _)| t.hashed() == *hash);
        if let Some(pos) = tx {
            self.store.unsettled_txs.remove(pos);
            return Some(pos);
        }
        None
    }

    fn get_state_of_prev_tx(&self, tx: &TxHash) -> Option<Contract> {
        let prev_tx = self
            .store
            .tx_chain
            .iter()
            .enumerate()
            .find(|(_, h)| *h == tx)
            .and_then(|(i, _)| {
                if i > 0 {
                    self.store.tx_chain.get(i - 1)
                } else {
                    None
                }
            });
        if let Some(prev_tx) = prev_tx {
            let prev_state = self.store.state_history.get(prev_tx).cloned();
            if let Some(contract) = prev_state {
                debug!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx,
                    "Found previous state from tx {:?}",
                    prev_tx
                );
                return Some(contract.0);
            } else {
                error!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx,
                    "No state history for previous tx {:?}, returning None",
                    prev_tx
                );
                error!("This is likely a bug in the prover, please report it to the Hyle team.");
                error!(cn =% self.ctx.contract_name, tx_hash =% tx, "State history: {:?}", self.store.state_history.keys());
                error!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx,
                    "Unsettled txs: {:?}",
                    self.store.unsettled_txs.iter().map(|(t, _)| t.hashed()).collect::<Vec<_>>()
                );
            }
        } else {
            warn!(cn =% self.ctx.contract_name, tx_hash =% tx, "No previous tx, returning default state");
            return Some(self.ctx.default_state.clone());
        }
        None
    }

    fn handle_all_next_blobs_after_failed(&mut self, idx: usize) -> Result<()> {
        let mut blobs = vec![];
        for (tx, ctx) in self.store.unsettled_txs.clone().iter().skip(idx) {
            for (index, blob) in tx.blobs.iter().enumerate() {
                if blob.contract_name == self.ctx.contract_name {
                    debug!(
                        cn =% self.ctx.contract_name,
                        "Re-execute blob for tx {} after a previous tx failure",
                        tx.hashed()
                    );
                    debug!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx.hashed(),
                        "🔥 Re-execute tx after failure, removing state history for tx {}",
                       tx.hashed()
                    );

                    self.store.state_history.remove(&tx.hashed());
                    blobs.push((index.into(), tx.clone(), ctx.clone()));
                }
            }
        }
        self.prove_supported_blob(blobs)
    }

    fn prove_supported_blob(
        &mut self,
        mut blobs: Vec<(BlobIndex, BlobTransaction, TxContext)>,
    ) -> Result<()> {
        let remaining_blobs = if blobs.len() > self.ctx.max_txs_per_proof {
            let remaining_blobs = blobs.split_off(self.ctx.max_txs_per_proof);
            info!(
                cn =% self.ctx.contract_name,
                "Too many blobs to prove in one go: {} / {}. Splitting into multiple proofs. ({} remaining)",
                blobs.len() + remaining_blobs.len(),
                self.ctx.max_txs_per_proof,
                remaining_blobs.len()
            );
            remaining_blobs
        } else {
            vec![]
        };

        let blobs = blobs; // remove mut

        if blobs.is_empty() {
            return Ok(());
        }
        let batch_id = self.store.batch_id;
        self.store.batch_id += 1;
        debug!(
            cn =% self.ctx.contract_name,
            "Handling {} txs. Batch ID: {batch_id}",
            blobs.len()
        );
        let mut calldatas = vec![];
        let mut initial_commitment_metadata = None;
        let len = blobs.len();
        for (blob_index, tx, tx_ctx) in blobs {
            let blob = tx.blobs.get(blob_index.0).ok_or_else(|| {
                anyhow!("Failed to get blob {} from tx {}", blob_index, tx.hashed())
            })?;
            let blobs = tx.blobs.clone();
            let tx_hash = tx.hashed();

            let mut contract = self
                .get_state_of_prev_tx(&tx_hash)
                .ok_or_else(|| anyhow!("Failed to get state of previous tx {}", tx_hash))?;

            let initial_contract = contract.clone();

            let state = contract
                .build_commitment_metadata(blob)
                .map_err(|e| anyhow!(e))
                .context("Failed to build commitment metadata");

            // If failed to build commitment metadata, we skip the tx, but continue with next ones
            if let Err(e) = state {
                error!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx.hashed(),
                    tx_height =% tx_ctx.block_height,
                    "{e:#}"
                );
                self.bus
                    .send(AutoProverEvent::FailedTx(tx_hash.clone(), e.to_string()))?;
                continue;
            }
            let state = state.unwrap();

            let commitment_metadata = state;

            if initial_commitment_metadata.is_none() {
                initial_commitment_metadata = Some(commitment_metadata.clone());
            } else {
                initial_commitment_metadata = Some(
                    contract
                        .merge_commitment_metadata(
                            initial_commitment_metadata.unwrap(),
                            commitment_metadata.clone(),
                        )
                        .map_err(|e| anyhow!(e))
                        .context("Merging commitment_metadata")?,
                );
            }

            let calldata = Calldata {
                identity: tx.identity.clone(),
                tx_hash: tx_hash.clone(),
                private_input: vec![],
                blobs: blobs.clone().into(),
                index: blob_index,
                tx_ctx: Some(tx_ctx.clone()),
                tx_blob_count: blobs.len(),
            };

            match contract.handle(&calldata).map_err(|e| anyhow!(e)) {
                Err(e) => {
                    info!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx.hashed(),
                        tx_height =% tx_ctx.block_height,
                        "Error while executing contract: {e}"
                    );
                    self.bus
                        .send(AutoProverEvent::FailedTx(tx_hash.clone(), e.to_string()))?;
                }
                Ok(hyle_output) => {
                    info!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx.hashed(),
                        tx_height =% tx_ctx.block_height,
                        "🔧 Executed contract: {}. Success: {}",
                        String::from_utf8_lossy(&hyle_output.program_outputs),
                        hyle_output.success
                    );
                    self.bus.send(AutoProverEvent::SuccessTx(
                        tx_hash.clone(),
                        contract.clone(),
                    ))?;
                    if !hyle_output.success {
                        debug!(
                            cn =% self.ctx.contract_name,
                            tx_hash =% tx.hashed(),
                            tx_height =% tx_ctx.block_height,
                            "Tx {} failed, storing initial state",
                            tx.hashed()
                        );
                        self.store
                            .state_history
                            .insert(tx_hash.clone(), (initial_contract, false));
                    } else {
                        debug!(
                            cn =% self.ctx.contract_name,
                            tx_hash =% tx.hashed(),
                            tx_height =% tx_ctx.block_height,
                            "Adding state history for tx {}",
                            tx.hashed()
                        );
                        self.store
                            .state_history
                            .insert(tx_hash.clone(), (contract.clone(), true));
                    }
                }
            }

            calldatas.push(calldata);
        }

        if calldatas.is_empty() {
            self.prove_supported_blob(remaining_blobs)?;
            return Ok(());
        }

        let Some(commitment_metadata) = initial_commitment_metadata else {
            self.prove_supported_blob(remaining_blobs)?;
            return Ok(());
        };

        let node_client = self.ctx.node.clone();
        let prover = self.ctx.prover.clone();
        let contract_name = self.ctx.contract_name.clone();

        let metrics = self.metrics.clone();
        logged_task(async move {
            let mut retries = 0;
            const MAX_RETRIES: u32 = 30;

            loop {
                debug!(
                    cn =% contract_name,
                    "Proving {} txs. Batch id: {batch_id}, Retries: {retries}",
                    calldatas.len(),
                );
                let start = std::time::Instant::now();
                metrics.record_proof_requested();
                match prover
                    .prove(commitment_metadata.clone(), calldatas.clone())
                    .await
                {
                    Ok(proof) => {
                        let elapsed = start.elapsed();
                        metrics.record_generation_time(elapsed.as_secs_f64());
                        metrics.record_proof_size(proof.0.len() as u64);
                        metrics.record_proof_success();
                        let tx = ProofTransaction {
                            contract_name: contract_name.clone(),
                            proof,
                        };
                        match node_client.send_tx_proof(tx).await {
                            Ok(tx_hash) => {
                                info!("✅ Proved {len} txs, Batch id: {batch_id}, Proof TX hash: {tx_hash}");
                            }
                            Err(e) => {
                                error!("Failed to send proof: {e:#}");
                            }
                        }
                        break;
                    }
                    Err(e) => {
                        metrics.record_proof_failure();

                        let should_retry =
                            e.to_string().contains("SessionCreateErr") && retries < MAX_RETRIES;
                        if should_retry {
                            warn!(
                                "Batch id: {batch_id}, Session creation error, retrying ({}/{}). {e:#}",
                                retries, MAX_RETRIES
                            );
                            retries += 1;
                            metrics.record_proof_retry();
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                        error!("Error proving tx: {:?}. Batch id: {batch_id}", e);
                        break;
                    }
                };
            }
        });
        self.prove_supported_blob(remaining_blobs)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bus::metrics::BusMetrics,
        node_state::{
            test::{make_hyle_output_with_state, new_node_state, new_proof_tx},
            NodeState,
        },
    };

    use super::*;
    use client_sdk::helpers::test::TxExecutorTestProver;
    use client_sdk::rest_client::test::NodeApiMockClient;
    use sdk::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
    struct TestContract {
        value: u32,
    }

    impl ZkContract for TestContract {
        fn execute(&mut self, calldata: &Calldata) -> sdk::RunResult {
            let (action, execution_ctx) = sdk::utils::parse_raw_calldata::<u32>(calldata)?;
            tracing::info!(
                tx_hash =% calldata.tx_hash,
                "Executing contract (val = {}) with action: {:?}",
                self.value,
                action,
            );
            self.value += action;
            if calldata.identity.0.starts_with("failing_") {
                return Err("This transaction is failing".to_string());
            }
            Ok(("ok".to_string().into_bytes(), execution_ctx, vec![]))
        }

        fn commit(&self) -> sdk::StateCommitment {
            sdk::StateCommitment(
                borsh::to_vec(self)
                    .map_err(|e| anyhow!(e))
                    .context("Failed to commit state")
                    .unwrap(),
            )
        }
    }

    impl TxExecutorHandler for TestContract {
        fn build_commitment_metadata(&self, blob: &Blob) -> Result<Vec<u8>> {
            let action = borsh::from_slice::<u32>(&blob.data.0)
                .context("Failed to parse action from blob data")?;
            if action == 66 {
                return Err(anyhow!("Order 66 is forbidden. Jedi are safe."));
            }
            borsh::to_vec(self).map_err(Into::into)
        }

        fn handle(&mut self, calldata: &Calldata) -> Result<sdk::HyleOutput> {
            let initial_state = ZkContract::commit(self);
            let mut res = self.execute(calldata);
            let next_state = ZkContract::commit(self);
            Ok(sdk::utils::as_hyle_output(
                initial_state,
                next_state,
                calldata,
                &mut res,
            ))
        }

        fn construct_state(
            _register_blob: &RegisterContractEffect,
            _metadata: &Option<Vec<u8>>,
        ) -> Result<Self> {
            Ok(Self::default())
        }
        fn get_state_commitment(&self) -> StateCommitment {
            self.commit()
        }
    }

    async fn setup_with_timeout(
        timeout: u64,
    ) -> Result<(NodeState, AutoProver<TestContract>, Arc<NodeApiMockClient>)> {
        let mut node_state = new_node_state().await;
        let register = RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: TestContract::default().commit(),
            contract_name: "test".into(),
            timeout_window: Some(TimeoutWindow::Timeout(BlockHeight(timeout))),
        };
        node_state.handle_register_contract_effect(&register);

        let api_client = Arc::new(NodeApiMockClient::new());

        let auto_prover = new_simple_auto_prover(api_client.clone()).await?;

        Ok((node_state, auto_prover, api_client))
    }

    async fn setup() -> Result<(NodeState, AutoProver<TestContract>, Arc<NodeApiMockClient>)> {
        setup_with_timeout(5).await
    }

    async fn new_simple_auto_prover(
        api_client: Arc<NodeApiMockClient>,
    ) -> Result<AutoProver<TestContract>> {
        new_buffering_auto_prover(api_client, 0, 100).await
    }

    async fn new_buffering_auto_prover(
        api_client: Arc<NodeApiMockClient>,
        buffer_blocks: u32,
        max_txs_per_proof: usize,
    ) -> Result<AutoProver<TestContract>> {
        let temp_dir = tempdir()?;
        let data_dir = temp_dir.path().to_path_buf();
        let ctx = Arc::new(AutoProverCtx {
            data_directory: data_dir,
            prover: Arc::new(TxExecutorTestProver::<TestContract>::new()),
            contract_name: ContractName("test".into()),
            node: api_client,
            default_state: TestContract::default(),
            buffer_blocks,
            max_txs_per_proof,
        });

        let bus = SharedMessageBus::new(BusMetrics::global("default".to_string()));
        AutoProver::<TestContract>::build(bus.new_handle(), ctx).await
    }

    async fn get_txs(api_client: &Arc<NodeApiMockClient>) -> Vec<Transaction> {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut gard = api_client.pending_proofs.lock().unwrap();
        let txs = gard.drain(..).collect::<Vec<ProofTransaction>>();
        txs.into_iter()
            .map(|t| {
                let hyle_outputs = borsh::from_slice::<Vec<HyleOutput>>(&t.proof.0)
                    .context("parsing test proof")
                    .unwrap();
                for hyle_output in &hyle_outputs {
                    tracing::info!(
                        "Initial state: {:?}, Next state: {:?}",
                        hyle_output.initial_state,
                        hyle_output.next_state
                    );
                }

                let proven_blobs = hyle_outputs
                    .into_iter()
                    .map(|hyle_output| {
                        let blob_tx_hash = hyle_output.tx_hash.clone();
                        BlobProofOutput {
                            hyle_output,
                            program_id: ProgramId(vec![]),
                            blob_tx_hash,
                            original_proof_hash: t.proof.hashed(),
                        }
                    })
                    .collect();
                VerifiedProofTransaction {
                    contract_name: t.contract_name.clone(),
                    proven_blobs,
                    proof_hash: t.proof.hashed(),
                    proof_size: t.estimate_size(),
                    proof: Some(t.proof),
                    is_recursive: false,
                }
                .into()
            })
            .collect()
    }

    fn count_hyle_outputs(proof: &Transaction) -> usize {
        if let TransactionData::VerifiedProof(VerifiedProofTransaction { proven_blobs, .. }) =
            &proof.transaction_data
        {
            proven_blobs.len()
        } else {
            tracing::info!("No Hyle outputs in this transaction");
            0
        }
    }

    fn new_blob_tx(val: u32) -> Transaction {
        // random id to have a different tx hash
        let id: usize = rand::random();
        let tx = BlobTransaction::new(
            format!("{id}@test"),
            vec![Blob {
                contract_name: "test".into(),
                data: BlobData(borsh::to_vec(&val).unwrap()),
            }],
        );
        tracing::info!(
            "📦️ Created new blob tx: {} with value: {}",
            tx.hashed(),
            val
        );
        tx.into()
    }

    fn new_failing_blob_tx(val: u32) -> Transaction {
        // random id to have a different tx hash
        let id: usize = rand::random();
        let tx = BlobTransaction::new(
            format!("failing_{id}@test"),
            vec![Blob {
                contract_name: "test".into(),
                data: BlobData(borsh::to_vec(&val).unwrap()),
            }],
        );
        tracing::info!(
            "📦️ Created new failing blob tx: {} with value: {}",
            tx.hashed(),
            val
        );
        tx.into()
    }

    fn read_contract_state(node_state: &NodeState) -> TestContract {
        let state = node_state
            .contracts
            .get(&"test".into())
            .unwrap()
            .state
            .clone();

        borsh::from_slice::<TestContract>(&state.0).expect("Failed to decode contract state")
    }

    impl<Contract> AutoProver<Contract>
    where
        Contract: TxExecutorHandler + Debug + Clone + Send + Sync + 'static,
    {
        async fn handle_block(&mut self, block: Block) -> Result<()> {
            self.handle_node_state_event(NodeStateEvent::NewBlock(Box::new(block)))
                .await
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_simple() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);

        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        tracing::info!("✨ Block 2");
        let block_2 = node_state.craft_block_and_handle(2, proofs);
        auto_prover.handle_processed_block(block_2).await?;

        assert_eq!(read_contract_state(&node_state).value, 1);

        tracing::info!("✨ Block 3");
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3), new_blob_tx(3)]);
        auto_prover.handle_processed_block(block_3).await?;
        let proofs_3 = get_txs(&api_client).await;
        assert_eq!(proofs_3.len(), 1);
        tracing::info!("✨ Block 4");
        let block_4 = node_state.craft_block_and_handle(4, proofs_3);
        auto_prover.handle_processed_block(block_4).await?;
        assert_eq!(read_contract_state(&node_state).value, 1 + 3 + 3);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_basic() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);

        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        tracing::info!("✨ Block 2");
        let block_2 = node_state.craft_block_and_handle(2, proofs);
        auto_prover.handle_processed_block(block_2).await?;

        assert_eq!(read_contract_state(&node_state).value, 1);

        tracing::info!("✨ Block 3");
        let block_3 = node_state.craft_block_and_handle(
            3,
            vec![
                new_blob_tx(3), /* this one will timeout */
                new_blob_tx(3), /* this one will timeout */
                new_blob_tx(3),
            ],
        );
        auto_prover.handle_processed_block(block_3).await?;

        // Proofs 3 won't be sent, to trigger a timeout
        let proofs_3 = get_txs(&api_client).await;
        assert_eq!(proofs_3.len(), 1);

        tracing::info!("✨ Block 4");
        let block_4 = node_state
            .craft_block_and_handle(4, vec![new_blob_tx(4), new_blob_tx(4), new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4).await?;
        let proofs_4 = get_txs(&api_client).await;
        assert_eq!(proofs_4.len(), 1);

        for i in 5..15 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 2);

        let _block_11 = node_state.craft_block_and_handle(16, proofs);
        assert_eq!(read_contract_state(&node_state).value, 16);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_tx_failed() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_failing_blob_tx(1)]);

        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        tracing::info!("✨ Block 2");
        node_state.craft_block_and_handle(2, proofs);

        assert_eq!(read_contract_state(&node_state).value, 0);

        tracing::info!("✨ Block 3");
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        auto_prover.handle_processed_block(block_3).await?;

        let proofs_3 = get_txs(&api_client).await;
        assert_eq!(proofs_3.len(), 1);

        tracing::info!("✨ Block 4");
        node_state.craft_block_and_handle(4, proofs_3);

        assert_eq!(read_contract_state(&node_state).value, 3);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_tx_middle_failed() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);

        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        tracing::info!("✨ Block 2");
        node_state.craft_block_and_handle(2, proofs);

        assert_eq!(read_contract_state(&node_state).value, 1);

        tracing::info!("✨ Block 3");
        let block_3 = node_state.craft_block_and_handle(
            3,
            vec![
                new_failing_blob_tx(3),
                new_blob_tx(3),
                new_failing_blob_tx(3),
                new_failing_blob_tx(3),
                new_failing_blob_tx(3),
                new_failing_blob_tx(3),
                new_blob_tx(3),
                new_failing_blob_tx(3),
                new_failing_blob_tx(3),
            ],
        );
        auto_prover.handle_processed_block(block_3).await?;

        let proofs_3 = get_txs(&api_client).await;
        assert_eq!(proofs_3.len(), 1);

        tracing::info!("✨ Block 4");
        node_state.craft_block_and_handle(4, proofs_3);

        assert_eq!(read_contract_state(&node_state).value, 1 + 3 + 3);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_tx_failed_after_success_in_same_block() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup_with_timeout(10).await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_failing_blob_tx(3)]);
        let block_4 = node_state.craft_block_and_handle(4, vec![new_failing_blob_tx(4)]);
        let block_5 = node_state.craft_block_and_handle(5, vec![new_blob_tx(5)]);

        let blocks = vec![block_1, block_2, block_3, block_4, block_5];
        for block in blocks {
            auto_prover.handle_processed_block(block).await?;
        }
        // All proofs needs to arrive in the same block to raise the error
        let proofs = get_txs(&api_client).await;
        let block_6 = node_state.craft_block_and_handle(6, proofs);
        auto_prover.handle_processed_block(block_6).await?;
        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 5);
        tracing::info!("✨ Block 7");
        let block_7 = node_state.craft_block_and_handle(7, vec![new_blob_tx(7)]);
        auto_prover.handle_processed_block(block_7).await?;
        let proofs_7 = get_txs(&api_client).await;
        tracing::info!("✨ Block 8");
        let block_8 = node_state.craft_block_and_handle(8, proofs_7);
        auto_prover.handle_processed_block(block_8).await?;
        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 5 + 7);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_lot_tx_failed() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(
            1,
            vec![
                new_failing_blob_tx(1),
                new_failing_blob_tx(1),
                new_failing_blob_tx(1),
            ],
        );

        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        tracing::info!("✨ Block 2");
        node_state.craft_block_and_handle(2, proofs);

        assert_eq!(read_contract_state(&node_state).value, 0);

        tracing::info!("✨ Block 3");
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        auto_prover.handle_processed_block(block_3).await?;

        let proofs_3 = get_txs(&api_client).await;
        assert_eq!(proofs_3.len(), 1);

        tracing::info!("✨ Block 4");
        node_state.craft_block_and_handle(4, proofs_3);

        assert_eq!(read_contract_state(&node_state).value, 3);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_instant_failed() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let tx = BlobTransaction::new(
            "yolo@test".to_string(),
            vec![
                Blob {
                    contract_name: "doesnotexist".into(),
                    data: BlobData(borsh::to_vec(&3).unwrap()),
                },
                Blob {
                    contract_name: "test".into(),
                    data: BlobData(borsh::to_vec(&3).unwrap()),
                },
            ],
        );

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![tx.into(), new_blob_tx(1)]);
        auto_prover.handle_processed_block(block_1).await?;
        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        tracing::info!("✨ Block 2");
        let block_2 = node_state.craft_block_and_handle(2, proofs);
        auto_prover.handle_processed_block(block_2).await?;
        assert_eq!(read_contract_state(&node_state).value, 1);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_tx_commitment_metadata_failed() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(66)]);

        let _ = auto_prover.handle_processed_block(block_1).await;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 0);

        for i in 2..7 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            auto_prover.handle_processed_block(block).await?;
        }

        tracing::info!("✨ Block 7");
        let block_7 = node_state.craft_block_and_handle(7, vec![new_blob_tx(7)]);
        auto_prover.handle_processed_block(block_7).await?;

        let proofs_7 = get_txs(&api_client).await;
        assert_eq!(proofs_7.len(), 1);

        tracing::info!("✨ Block 8");
        let block_8 = node_state.craft_block_and_handle(8, proofs_7);
        auto_prover.handle_processed_block(block_8).await?;

        assert_eq!(read_contract_state(&node_state).value, 7);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_n() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 = node_state.craft_block_and_handle(6, vec![new_blob_tx(6)]);
        let block_7 = node_state.craft_block_and_handle(7, vec![new_blob_tx(7)]);
        let block_8 = node_state.craft_block_and_handle(8, vec![new_blob_tx(8)]);

        tracing::info!("✨ New prover catching up with blocks 6 and 7");
        api_client.set_block_height(BlockHeight(7));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        auto_prover_catchup.handle_block(block_1.clone()).await?;
        auto_prover_catchup.handle_block(block_2.clone()).await?;
        auto_prover_catchup.handle_block(block_3.clone()).await?;
        auto_prover_catchup.handle_block(block_4.clone()).await?;
        auto_prover_catchup.handle_block(block_5.clone()).await?;
        auto_prover_catchup.handle_block(block_6.clone()).await?;
        auto_prover_catchup.handle_block(block_7.clone()).await?;
        auto_prover_catchup.handle_block(block_8.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1); // Txs from mutliple catching blocs are batched
        let _ = node_state.craft_block_and_handle(9, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 6 + 7 + 8);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_timeout_1() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 = node_state.craft_block_and_handle(
            6,
            vec![
                new_blob_tx(6), /* This one will timeout on block 11 */
                new_blob_tx(6), /* This one will timeout on block 16*/
                new_blob_tx(6),
                new_blob_tx(6),
            ],
        );

        let block_7 = node_state.craft_block_and_handle(7, vec![new_blob_tx(7)]);
        let block_8 = node_state.craft_block_and_handle(8, vec![new_blob_tx(8)]);

        let mut blocks = vec![
            block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8,
        ];
        for i in 9..20 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        tracing::info!("✨ New prover catching up with blocks");
        // After the second one times out.
        api_client.set_block_height(BlockHeight(17));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1); // Txs from mutliple catching blocs are batched
        assert_eq!(count_hyle_outputs(&proofs[0]), 4);
        let _ = node_state.craft_block_and_handle(20, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 6 + 6 + 7 + 8);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_timeout_2() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 = node_state.craft_block_and_handle(
            6,
            vec![
                new_blob_tx(6), /* This one will timeout */
                new_blob_tx(6), /* This one will timeout */
                new_blob_tx(6), /* This one will timeout in first catching block */
                new_blob_tx(6),
            ],
        );

        let block_7 = node_state.craft_block_and_handle(7, vec![new_blob_tx(7)]);
        let block_8 = node_state.craft_block_and_handle(8, vec![new_blob_tx(8)]);

        let mut blocks = vec![
            block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8,
        ];
        let stop_height = 22;
        for i in 9..stop_height {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        tracing::info!("✨ New prover catching up");
        api_client.set_block_height(BlockHeight(stop_height - 1));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_block(block).await?;
        }
        // One more block to trigger proof generation
        let block = node_state.craft_block_and_handle(stop_height, vec![]);
        auto_prover_catchup.handle_block(block).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        assert_eq!(count_hyle_outputs(&proofs[0]), 3);
        let _ = node_state.craft_block_and_handle(stop_height + 1, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 6 + 7 + 8);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_timeout_multiple_blocks() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 =
            node_state.craft_block_and_handle(6, vec![new_blob_tx(6) /* This one will timeout */]);

        let block_7 =
            node_state.craft_block_and_handle(7, vec![new_blob_tx(7) /* This one will timeout */]);
        let block_8 =
            node_state.craft_block_and_handle(8, vec![new_blob_tx(8) /* This one will timeout */]);
        let block_9 = node_state.craft_block_and_handle(9, vec![new_blob_tx(9)]);
        let block_10 = node_state.craft_block_and_handle(10, vec![new_blob_tx(10)]);

        let mut blocks = vec![
            block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8, block_9,
            block_10,
        ];
        let stop_height = 24;
        for i in 11..stop_height {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        tracing::info!("✨ New prover catching up");
        api_client.set_block_height(BlockHeight(stop_height - 1));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_block(block).await?;
        }
        // One more block to trigger proof generation
        let block = node_state.craft_block_and_handle(stop_height, vec![]);
        auto_prover_catchup.handle_block(block).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1); // Txs from mutliple catching blocs are batched
        assert_eq!(count_hyle_outputs(&proofs[0]), 2);
        let _ = node_state.craft_block_and_handle(stop_height + 1, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 9 + 10);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_timeout_between_settled() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 =
            node_state.craft_block_and_handle(6, vec![new_blob_tx(6) /* This one will timeout */]);

        let block_7 =
            node_state.craft_block_and_handle(7, vec![new_blob_tx(7) /* This one will timeout */]);
        let block_8 =
            node_state.craft_block_and_handle(8, vec![new_blob_tx(8) /* This one will timeout */]);
        let block_9 = node_state.craft_block_and_handle(9, vec![new_blob_tx(9)]);
        let block_10 = node_state.craft_block_and_handle(10, vec![new_blob_tx(10)]);

        let mut blocks = vec![
            block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8, block_9,
            block_10,
        ];
        let stop_height = 24;
        for i in 11..stop_height {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        for i in 6..stop_height {
            tracing::info!("♻️ Handle block {}", i);
            auto_prover
                .handle_processed_block(blocks[i as usize - 1].clone())
                .await?;
        }
        let proofs = get_txs(&api_client).await;
        let block_24 = node_state.craft_block_and_handle(stop_height, proofs);
        let block_25 = node_state.craft_block_and_handle(stop_height + 1, vec![new_blob_tx(25)]);
        blocks.push(block_24);
        blocks.push(block_25);

        tracing::info!("✨ New prover catching up");
        api_client.set_block_height(BlockHeight(stop_height + 1));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_block(block).await?;
        }
        // One more block to trigger proof generation
        let block = node_state.craft_block_and_handle(stop_height + 2, vec![]);
        auto_prover_catchup.handle_block(block).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        let _ = node_state.craft_block_and_handle(stop_height + 3, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 9 + 10 + 25);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_first_txs_timeout() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block_1 =
            node_state.craft_block_and_handle(1, vec![new_blob_tx(1) /* This one will timeout */]);
        let block_2 =
            node_state.craft_block_and_handle(2, vec![new_blob_tx(2) /* This one will timeout */]);
        let block_3 =
            node_state.craft_block_and_handle(3, vec![new_blob_tx(3) /* This one will timeout */]);

        let mut blocks = vec![block_1, block_2, block_3];
        let stop_height = 20;
        for i in 4..=stop_height {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        tracing::info!("✨ New prover catching up with blocks");
        api_client.set_block_height(BlockHeight(stop_height - 1));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 0);

        tracing::info!("✨ Block 21");
        let block_21 = node_state.craft_block_and_handle(21, vec![new_blob_tx(21)]);

        auto_prover_catchup.handle_block(block_21.clone()).await?;
        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        assert_eq!(count_hyle_outputs(&proofs[0]), 1);
        tracing::info!("✨ Block 22");
        let _ = node_state.craft_block_and_handle(22, proofs);

        assert_eq!(read_contract_state(&node_state).value, 21);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_buffer_2_blocks() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);

        let blocks = vec![block_1, block_2, block_3, block_4];

        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 2, 100).await?;

        for block in blocks.clone() {
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        tracing::info!("✨ Block 5");
        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5).await?;

        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3);

        let block_6 = node_state.craft_block_and_handle(6, vec![new_blob_tx(6)]);
        auto_prover.handle_processed_block(block_6).await?;
        let proofs_6 = get_txs(&api_client).await;
        assert_eq!(proofs_6.len(), 1);
        tracing::info!("✨ Block 7");
        let _ = node_state.craft_block_and_handle(7, proofs_6);
        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3 + 4 + 6);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_buffer_2_txs() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);

        let blocks = vec![block_1, block_2, block_3, block_4];

        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 100, 3).await?;

        for block in blocks.clone() {
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        tracing::info!("✨ Block 5");
        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5).await?;

        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3);

        let block_6 = node_state.craft_block_and_handle(6, vec![new_blob_tx(6), new_blob_tx(6)]);
        auto_prover.handle_processed_block(block_6).await?;
        let proofs_6 = get_txs(&api_client).await;
        assert_eq!(proofs_6.len(), 1);
        tracing::info!("✨ Block 7");
        let _ = node_state.craft_block_and_handle(7, proofs_6);
        assert_eq!(
            read_contract_state(&node_state).value,
            1 + 2 + 3 + 4 + 6 + 6
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_buffer_max_txs_per_proof() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);

        let blocks = vec![block_1, block_2, block_3, block_4];

        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 100, 2).await?;

        for block in blocks.clone() {
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 2);
        tracing::info!("✨ Block 5");
        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5).await?;

        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3 + 4);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_buffer_one_block_max_txs_per_proof() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block = node_state.craft_block_and_handle(
            1,
            vec![
                new_blob_tx(1),
                new_blob_tx(2),
                new_blob_tx(3),
                new_blob_tx(4),
            ],
        );

        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 100, 2).await?;

        auto_prover.handle_processed_block(block).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 2);
        tracing::info!("✨ Block 5");
        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5).await?;

        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3 + 4);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_artificial_middle_blob_failure_nobuffering() -> Result<()> {
        let node_state = new_node_state().await;
        let api_client = Arc::new(NodeApiMockClient::new());
        let auto_prover = new_simple_auto_prover(api_client.clone()).await?;

        scenario_auto_prover_artificial_middle_blob_failure(node_state, api_client, auto_prover)
            .await
            .expect("Failed to run scenario");

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_artificial_middle_blob_failure_buffering() -> Result<()> {
        let node_state = new_node_state().await;
        let api_client = Arc::new(NodeApiMockClient::new());
        let auto_prover = new_buffering_auto_prover(api_client.clone(), 10, 20).await?;

        scenario_auto_prover_artificial_middle_blob_failure(node_state, api_client, auto_prover)
            .await
            .expect("Failed to run scenario");

        Ok(())
    }

    async fn scenario_auto_prover_artificial_middle_blob_failure(
        mut node_state: NodeState,
        api_client: Arc<NodeApiMockClient>,
        mut auto_prover: AutoProver<TestContract>,
    ) -> Result<()> {
        let register = RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: TestContract::default().commit(),
            contract_name: "test".into(),
            timeout_window: Some(TimeoutWindow::Timeout(BlockHeight(20))),
        };
        node_state.handle_register_contract_effect(&register);

        let register = RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: TestContract::default().commit(),
            contract_name: "test2".into(),
            timeout_window: Some(TimeoutWindow::Timeout(BlockHeight(20))),
        };
        node_state.handle_register_contract_effect(&register);

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;

        tracing::info!("✨ Block 2");
        let block_2 = node_state.craft_block_and_handle(2, proofs);
        auto_prover.handle_processed_block(block_2).await?;

        tracing::info!("✨ Block 3");
        // Create a batch of valid txs
        let mut txs: Vec<Transaction> = (0..7).map(|i| new_blob_tx(10 + i)).collect();

        let first_tx = BlobTransaction::new(
            Identity::new("toto@test2"),
            vec![Blob {
                contract_name: "test2".into(),
                data: BlobData(vec![1, 2, 3]),
            }],
        );

        txs.insert(0, first_tx.clone().into());

        let TransactionData::Blob(failing_tx_data) = txs[3].transaction_data.clone() else {
            panic!("Expected Blob transaction data");
        };

        let failing_tx_data = BlobTransaction::new(
            failing_tx_data.identity.clone(),
            vec![
                Blob {
                    contract_name: "test2".into(),
                    data: BlobData(vec![4, 5, 6]),
                },
                failing_tx_data.blobs[0].clone(),
            ],
        );
        tracing::info!("📦️ Creating failing TX: {:?}", failing_tx_data.hashed());
        txs[3] = failing_tx_data.clone().into();

        let mut ho =
            make_hyle_output_with_state(failing_tx_data.clone(), BlobIndex(0), &[4], &[34]);
        ho.success = false;
        let failing_proof =
            new_proof_tx(&ContractName::new("test2"), &ho, &failing_tx_data.hashed());

        let block_3 = node_state.craft_block_and_handle(3, txs);
        auto_prover.handle_processed_block(block_3).await?;

        tracing::info!("✨ Block 4");

        // We need to settle another TX first to trigger our own.
        let proof = new_proof_tx(
            &ContractName::new("test2"),
            &make_hyle_output_with_state(first_tx.clone(), BlobIndex(0), &[0, 0, 0, 0], &[4]),
            &first_tx.hashed(),
        );

        let proofs = get_txs(&api_client).await;

        let block_4 = node_state
            .craft_block_and_handle(4, vec![first_tx.into(), failing_proof.into(), proof.into()]);
        auto_prover.handle_processed_block(block_4).await?;

        tracing::info!("✨ Block 5");
        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5).await?;

        let proofs = get_txs(&api_client).await;

        tracing::info!("✨ Block 6");
        let block_6 = node_state.craft_block_and_handle(6, proofs);
        auto_prover.handle_processed_block(block_6).await?;

        for i in 7..12 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        let block = node_state.craft_block_and_handle(12, proofs);
        auto_prover.handle_processed_block(block).await?;

        assert_eq!(read_contract_state(&node_state).value, 80);

        assert_eq!(
            node_state.get_earliest_unsettled_height(&ContractName::new("test")),
            None
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]

    async fn test_auto_prover_early_fail_while_buffered() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;
        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 3, 20).await?;

        // Block 1: Failing TX
        tracing::info!("✨ Block 1");
        let failing_tx = new_failing_blob_tx(1);
        let block_1 = node_state.craft_block_and_handle(1, vec![failing_tx.clone()]);
        auto_prover.handle_processed_block(block_1).await?;

        // Process a few blocks to un-buffer the failing TX
        for i in 2..5 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            auto_prover.handle_processed_block(block).await?;
        }

        // Wait for the failing TX to be proven
        let proofs = get_txs(&api_client).await;

        // Block 2: Successful TX (should be buffered)
        tracing::info!("✨ Block 5");
        let success_tx = new_blob_tx(5);
        let block_5 = node_state.craft_block_and_handle(5, vec![success_tx.clone()]);
        auto_prover.handle_processed_block(block_5).await?;

        // Block 3: Simulate settlement of the failed TX from block 1
        tracing::info!("✨ Block 6 (settle fail)");
        let block_6 = node_state.craft_block_and_handle(6, proofs);
        auto_prover.handle_processed_block(block_6).await?;

        // Process a few blocks to un-buffer the failing TX
        for i in 7..9 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            auto_prover.handle_processed_block(block).await?;
        }

        // Now the buffered TX should be executed and a proof generated
        let proofs = get_txs(&api_client).await;

        tracing::info!("✨ Block 9");
        let block = node_state.craft_block_and_handle(9, proofs);
        auto_prover.handle_processed_block(block).await?;

        let success_tx = new_blob_tx(6);
        let hash = success_tx.hashed();

        tracing::info!("✨ Block 10");
        let block = node_state.craft_block_and_handle(10, vec![success_tx]);
        auto_prover.handle_processed_block(block).await?;

        // Process a few blocks to generate proof
        for i in 11..14 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;

        // Should settle the final TX
        tracing::info!("✨ Block 14");
        let block = node_state.craft_block_and_handle(14, proofs);
        assert_eq!(block.successful_txs, vec![hash]);
        assert!(node_state
            .get_earliest_unsettled_height(&ContractName::new("test"))
            .is_none(),);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_mixed_pending_and_failures() -> Result<()> {
        // Setup prover and node state
        let (mut node_state, _, api_client) = setup_with_timeout(20).await?;

        api_client.set_block_height(BlockHeight(5));
        api_client.add_contract(Contract {
            name: ContractName::new("test"),
            program_id: ProgramId(vec![4]),
            state: StateCommitment(vec![2, 0, 0, 0]),
            verifier: "test".into(),
            timeout_window: TimeoutWindow::Timeout(BlockHeight(20)),
        });

        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 0, 20).await?;

        // Block 1: Failing TX
        let failing_tx_1 = new_failing_blob_tx(1);
        let block_1 = node_state.craft_block_and_handle(1, vec![failing_tx_1.clone()]);
        auto_prover
            .handle_node_state_event(NodeStateEvent::NewBlock(Box::new(block_1.clone())))
            .await?;

        // Block 2: Successful TX
        let success_tx_2 = new_blob_tx(2);
        let block_2 = node_state.craft_block_and_handle(2, vec![success_tx_2.clone()]);
        auto_prover
            .handle_node_state_event(NodeStateEvent::NewBlock(Box::new(block_2.clone())))
            .await?;

        // Block 3: Pending TX (not settled yet, so not included in successful/failed/timed out)
        let pending_tx = new_blob_tx(3);
        let block_3 = node_state.craft_block_and_handle(3, vec![pending_tx.clone()]);
        auto_prover
            .handle_node_state_event(NodeStateEvent::NewBlock(Box::new(block_3.clone())))
            .await?;

        // Block 4: Failing TX, and result of 1/2/4
        let failing_tx_4 = new_failing_blob_tx(4);
        let mut block_4 = node_state.craft_block_and_handle(4, vec![failing_tx_4.clone()]);
        block_4.successful_txs = vec![success_tx_2.hashed()];
        block_4.timed_out_txs = vec![failing_tx_1.hashed()];
        block_4.failed_txs = vec![failing_tx_4.hashed()];
        block_4
            .dp_parent_hashes
            .insert(failing_tx_4.hashed(), DataProposalHash(format!("{}", 4)));
        block_4
            .dp_parent_hashes
            .insert(success_tx_2.hashed(), DataProposalHash(format!("{}", 2)));
        block_4
            .dp_parent_hashes
            .insert(failing_tx_1.hashed(), DataProposalHash(format!("{}", 1)));
        auto_prover
            .handle_node_state_event(NodeStateEvent::NewBlock(Box::new(block_4.clone())))
            .await?;

        // Block 5 is empty
        let block_5 = node_state.craft_block_and_handle(5, vec![]);
        auto_prover
            .handle_node_state_event(NodeStateEvent::NewBlock(Box::new(block_5.clone())))
            .await?;

        // Block 6: some other TX
        let other_tx = new_blob_tx(6);
        let block_6 = Block {
            block_height: BlockHeight(6),
            txs: vec![(
                TxId(DataProposalHash::default(), other_tx.hashed()),
                other_tx.clone(),
            )],
            successful_txs: vec![],
            failed_txs: vec![],
            timed_out_txs: vec![],
            lane_ids: BTreeMap::from_iter(vec![(
                other_tx.hashed(),
                LaneId(ValidatorPublicKey(vec![5])),
            )]),
            ..Default::default()
        };
        auto_prover
            .handle_node_state_event(NodeStateEvent::NewBlock(Box::new(block_6.clone())))
            .await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        // We can't actually process the proofs because node_state is faked in this test.
        // So just check that the state commitment is as expected.
        let TransactionData::VerifiedProof(proof) = proofs.last().unwrap().transaction_data.clone()
        else {
            panic!("Expected VerifiedProof transaction data");
        };
        assert_eq!(proof.proven_blobs.len(), 2);
        assert_eq!(
            proof.proven_blobs.last().unwrap().hyle_output.next_state,
            StateCommitment(vec![2 + 3 + 6, 0, 0, 0])
        );

        Ok(())
    }
}
