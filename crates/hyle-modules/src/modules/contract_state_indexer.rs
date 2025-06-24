use crate::{
    bus::{BusClientSender, SharedMessageBus},
    log_debug, log_error, module_bus_client, module_handle_messages,
    modules::Module,
};
use anyhow::{anyhow, Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::contract_indexer::{ContractHandler, ContractStateStore};
use sdk::*;
use std::{any::TypeId, ops::Deref, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tracing::debug;

use crate::node_state::module::NodeStateEvent;

use super::SharedBuildApiCtx;

#[derive(Debug, Clone)]
pub struct CSIBusEvent<E> {
    #[allow(unused)]
    pub event: E,
}

module_bus_client! {
#[derive(Debug)]
struct CSIBusClient<E: Clone + Send + Sync + 'static> {
    sender(CSIBusEvent<E>),
    receiver(NodeStateEvent),
}
}

pub struct ContractStateIndexer<State, Event: Clone + Send + Sync + 'static = ()> {
    bus: CSIBusClient<Event>,
    store: Arc<RwLock<ContractStateStore<State>>>,
    contract_name: ContractName,
    file: PathBuf,
}

pub struct ContractStateIndexerCtx {
    pub data_directory: PathBuf,
    pub contract_name: ContractName,
    pub api: SharedBuildApiCtx,
}

impl<State, Event> Module for ContractStateIndexer<State, Event>
where
    State: Clone
        + Sync
        + Send
        + std::fmt::Debug
        + ContractHandler<Event>
        + BorshSerialize
        + BorshDeserialize
        + 'static,
    Event: std::fmt::Debug + Clone + Send + Sync + 'static,
{
    type Context = ContractStateIndexerCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = CSIBusClient::new_from_bus(bus.new_handle()).await;
        let file = ctx
            .data_directory
            .join(format!("state_indexer_{}.bin", ctx.contract_name).as_str());

        let mut store =
            Self::load_from_disk_or_default::<ContractStateStore<State>>(file.as_path());
        store.contract_name = ctx.contract_name.clone();
        let store = Arc::new(RwLock::new(store));

        let (nested, mut api) = State::api(Arc::clone(&store)).await;
        if let Ok(mut o) = ctx.api.openapi.lock() {
            // Deduplicate operation ids
            for p in api.paths.paths.iter_mut() {
                p.1.get = p.1.get.take().map(|mut g| {
                    g.operation_id = g.operation_id.map(|o| format!("{}_{o}", ctx.contract_name));
                    g
                });
                p.1.post = p.1.post.take().map(|mut g| {
                    g.operation_id = g.operation_id.map(|o| format!("{}_{o}", ctx.contract_name));
                    g
                });
            }
            *o = o
                .clone()
                .nest(format!("/v1/indexer/contract/{}", ctx.contract_name), api);
        }

        if let Ok(mut guard) = ctx.api.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.nest(
                    format!("/v1/indexer/contract/{}", ctx.contract_name).as_str(),
                    nested,
                ));
            }
        }

        Ok(ContractStateIndexer {
            bus,
            file,
            store,
            contract_name: ctx.contract_name,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<NodeStateEvent> event => {
                _ = log_error!(
                    self.handle_node_state_event(event).await,
                    "Handling node state event"
                )
            }
        };

        Ok(())
    }

    async fn persist(&mut self) -> Result<()> {
        if let Err(e) = Self::save_on_disk::<ContractStateStore<State>>(
            self.file.as_path(),
            self.store.read().await.deref(),
        ) {
            tracing::warn!(cn = %self.contract_name, "Failed to save contract state indexer on disk: {}", e);
        }

        Ok(())
    }
}

impl<State, Event> ContractStateIndexer<State, Event>
where
    State: Clone
        + Sync
        + Send
        + std::fmt::Debug
        + ContractHandler<Event>
        + BorshSerialize
        + BorshDeserialize
        + 'static,
    Event: std::fmt::Debug + Clone + Send + Sync + 'static,
{
    /// Note: Each copy of the contract state indexer does the same handle_block on each data event
    /// coming from node state.
    async fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<(), Error> {
        let NodeStateEvent::NewBlock(block) = event;
        self.handle_processed_block(*block).await?;

        Ok(())
    }

    async fn handle_txs<'a, T: IntoIterator<Item = &'a TxHash>, F>(
        &mut self,
        txs: T,
        block: &Block,
        handler: F,
        remove_from_unsettled: bool,
    ) -> Result<()>
    where
        F: Fn(&mut State, &BlobTransaction, BlobIndex, TxContext) -> Result<Option<Event>>,
    {
        for tx in txs {
            let dp_hash = block.resolve_parent_dp_hash(tx)?.clone();
            let tx_id = TxId(dp_hash.clone(), tx.clone());

            let mut store = self.store.write().await;
            let (tx, tx_context) = match if remove_from_unsettled {
                store.unsettled_blobs.remove(&tx_id)
            } else {
                store.unsettled_blobs.get(&tx_id).cloned()
            } {
                Some(tx) => tx,
                None => {
                    debug!(cn = %self.contract_name, "🔨 No supported blobs found in transaction: {}", tx);
                    continue;
                }
            };

            let state = store
                .state
                .as_mut()
                .ok_or(anyhow!("No state found for {}", self.contract_name))?;

            for (index, Blob { contract_name, .. }) in tx.blobs.iter().enumerate() {
                if self.contract_name != *contract_name {
                    continue;
                }

                let event = handler(state, &tx, BlobIndex(index), tx_context.clone())?;
                if TypeId::of::<Event>() != TypeId::of::<()>() {
                    if let Some(event) = event {
                        let _ = log_debug!(
                            self.bus.send(CSIBusEvent {
                                event: event.clone(),
                            }),
                            "Sending CSI bus event"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    // Used in lieu of a closure below to work around a weird lifetime check issue.
    fn get_hash(tx: &(TxId, Transaction)) -> &TxHash {
        &tx.0 .1
    }

    async fn handle_processed_block(&mut self, block: Block) -> Result<()> {
        for (_, contract, metadata) in block.registered_contracts.values() {
            if self.contract_name == contract.contract_name {
                self.handle_register_contract(contract, metadata).await?;
            }
        }

        if !block.txs.is_empty() {
            debug!(handler = %self.contract_name, "🔨 Processing block: {}", block.block_height);
        }

        for (tx_id, tx) in &block.txs {
            if let TransactionData::Blob(tx) = &tx.transaction_data {
                let tx_context = block.build_tx_ctx(&tx.hashed())?;
                self.handle_blob(tx_id.clone(), tx.clone(), tx_context)
                    .await?;
            }
        }

        self.handle_txs(
            block.txs.iter().map(Self::get_hash),
            &block,
            |state, tx, index, ctx| state.handle_transaction_sequenced(tx, index, ctx),
            false,
        )
        .await?;

        self.handle_txs(
            &block.timed_out_txs,
            &block,
            |state, tx, index, ctx| state.handle_transaction_timeout(tx, index, ctx),
            true,
        )
        .await?;

        self.handle_txs(
            &block.failed_txs,
            &block,
            |state, tx, index, ctx| state.handle_transaction_failed(tx, index, ctx),
            true,
        )
        .await?;

        self.handle_txs(
            &block.successful_txs,
            &block,
            |state, tx, index, ctx| state.handle_transaction_success(tx, index, ctx),
            true,
        )
        .await?;

        Ok(())
    }

    async fn handle_blob(
        &mut self,
        tx_id: TxId,
        tx: BlobTransaction,
        tx_context: TxContext,
    ) -> Result<()> {
        let mut found_supported_blob = false;

        for b in &tx.blobs {
            if self.contract_name == b.contract_name {
                found_supported_blob = true;
                break;
            }
        }

        if found_supported_blob {
            debug!(cn = %self.contract_name, "⚒️  Found supported blob in transaction: {}", tx_id);
            self.store
                .write()
                .await
                .unsettled_blobs
                .insert(tx_id, (tx, tx_context));
        }

        Ok(())
    }

    async fn handle_register_contract(
        &self,
        contract: &RegisterContractEffect,
        metadata: &Option<Vec<u8>>,
    ) -> Result<()> {
        if let Some(state) = self.store.read().await.state.as_ref() {
            tracing::warn!(cn = %self.contract_name, "⚠️  Got re-register contract '{}'", contract.contract_name);
            if state.get_state_commitment() == contract.state_commitment {
                tracing::info!(cn = %self.contract_name, "📝 Re-register contract '{}' with same state commitment", contract.contract_name);
            } else {
                let state = State::construct_state(contract, metadata)?;
                tracing::warn!(cn = %self.contract_name, "📝 Contract '{}' re-built initial state", contract.contract_name);
                self.store.write().await.state = Some(state);
            }
        } else {
            let state = State::construct_state(contract, metadata)?;
            tracing::info!(cn = %self.contract_name, "📝 Registered suppored contract '{}'", contract.contract_name);
            self.store.write().await.state = Some(state);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use client_sdk::transaction_builder::TxExecutorHandler;
    use sdk::*;
    use utoipa::openapi::OpenApi;

    use super::*;
    use crate::bus::metrics::BusMetrics;
    use crate::bus::SharedMessageBus;
    use crate::node_state::metrics::NodeStateMetrics;
    use crate::node_state::{NodeState, NodeStateStore};
    use std::sync::Arc;

    #[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
    struct MockState(Vec<u8>);

    impl TryFrom<StateCommitment> for MockState {
        type Error = Error;

        fn try_from(value: StateCommitment) -> Result<Self> {
            Ok(MockState(value.0))
        }
    }

    impl ZkContract for MockState {
        fn execute(&mut self, _calldata: &Calldata) -> RunResult {
            Err("not implemented".into())
        }

        fn commit(&self) -> StateCommitment {
            StateCommitment(vec![])
        }
    }

    impl TxExecutorHandler for MockState {
        fn handle(&mut self, _: &Calldata) -> Result<HyleOutput> {
            anyhow::bail!("not implemented");
        }

        fn build_commitment_metadata(&self, _: &Blob) -> Result<Vec<u8>> {
            anyhow::bail!("not implemented");
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

    impl ContractHandler for MockState {
        fn handle_transaction_success(
            &mut self,
            tx: &BlobTransaction,
            index: BlobIndex,
            _tx_context: TxContext,
        ) -> Result<Option<()>> {
            self.0 = tx.blobs.get(index.0).unwrap().data.0.clone();
            Ok(None)
        }

        async fn api(_store: Arc<RwLock<ContractStateStore<Self>>>) -> (axum::Router<()>, OpenApi) {
            (axum::Router::new(), OpenApi::default())
        }
    }

    async fn build_indexer(contract_name: ContractName) -> ContractStateIndexer<MockState> {
        let ctx = ContractStateIndexerCtx {
            contract_name,
            data_directory: PathBuf::from("test_data"),
            api: Default::default(),
        };

        ContractStateIndexer::<MockState>::build(
            SharedMessageBus::new(BusMetrics::global("global".to_string())),
            ctx,
        )
        .await
        .unwrap()
    }

    async fn register_contract(indexer: &mut ContractStateIndexer<MockState>) {
        let state_commitment = StateCommitment::default();
        let rce = RegisterContractEffect {
            contract_name: indexer.contract_name.clone(),
            state_commitment,
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            ..Default::default()
        };
        indexer.handle_register_contract(&rce, &None).await.unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn test_handle_register_contract() {
        let contract_name = ContractName::from("test_contract");
        let mut indexer = build_indexer(contract_name.clone()).await;

        register_contract(&mut indexer).await;

        let store = indexer.store.read().await;
        assert!(store.state.is_some());
    }

    #[test_log::test(tokio::test)]
    async fn test_handle_blob() {
        let contract_name = ContractName::from("test_contract");
        let blob = Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![1, 2, 3]),
        };
        let tx = BlobTransaction::new("test", vec![blob]);
        let tx_hash = tx.hashed();
        let tx_context = TxContext::default();

        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;
        indexer
            .handle_blob(
                TxId(DataProposalHash::default(), tx_hash.clone()),
                tx,
                tx_context,
            )
            .await
            .unwrap();

        let store = indexer.store.read().await;
        assert!(store
            .unsettled_blobs
            .contains_key(&TxId(DataProposalHash::default(), tx_hash.clone())));
        assert!(store.state.clone().unwrap().0.is_empty());
    }

    #[test_log::test(tokio::test)]
    async fn test_settle_tx() {
        let contract_name = ContractName::from("test_contract");
        let blob = Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![1, 2, 3]),
        };
        let tx = BlobTransaction::new("test", vec![blob]);
        let tx_id = TxId(DataProposalHash::default(), tx.hashed());
        let tx_context = TxContext::default();

        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;
        {
            let mut store = indexer.store.write().await;
            store
                .unsettled_blobs
                .insert(tx_id.clone(), (tx, tx_context));
        }

        indexer
            .handle_txs(
                &[tx_id.1.clone()],
                &Block {
                    lane_ids: vec![(tx_id.1.clone(), LaneId::default())]
                        .into_iter()
                        .collect(),
                    dp_parent_hashes: vec![(tx_id.1.clone(), DataProposalHash::default())]
                        .into_iter()
                        .collect(),
                    ..Block::default()
                },
                |state, tx, index, ctx| state.handle_transaction_success(tx, index, ctx),
                true,
            )
            .await
            .unwrap();

        let store = indexer.store.read().await;
        assert!(!store.unsettled_blobs.contains_key(&tx_id));
        assert_eq!(store.state.clone().unwrap().0, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_handle_node_state_event() {
        let contract_name = ContractName::from("test_contract");
        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;

        let mut node_state = NodeState {
            metrics: NodeStateMetrics::global("test".to_string(), "test"),
            store: NodeStateStore::default(),
        };
        let block = node_state
            .handle_signed_block(&SignedBlock::default())
            .unwrap();

        let event = NodeStateEvent::NewBlock(Box::new(block));

        indexer.handle_node_state_event(event).await.unwrap();
        // Add assertions based on the expected state changes
    }
}
