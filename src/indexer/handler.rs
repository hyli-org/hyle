use super::api::*;
use crate::model::*;
use crate::node_state::module::NodeStateEvent;
use anyhow::{bail, Context, Error, Result};
use chrono::{DateTime, Utc};
use hyle_contract_sdk::TxHash;
use hyle_model::api::{TransactionStatusDb, TransactionTypeDb};
use hyle_model::utils::TimestampMs;
use hyle_modules::{log_error, log_warn};
use hyle_net::clock::TimestampMsClock;
use sqlx::Postgres;
use sqlx::QueryBuilder;
use sqlx::Row;
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;
use std::sync::Arc;
use tracing::{debug, info, trace};

use super::Indexer;

fn calculate_optimal_batch_size(params_per_item: usize) -> usize {
    let max_params: usize = 65000; // Security margin
    if params_per_item == 0 {
        return 1;
    }

    let optimal_size = max_params / params_per_item;
    // Assert there is at least 1 elem per batch
    std::cmp::max(1, optimal_size)
}

#[derive(Debug)]
pub struct TxDataStore {
    pub tx_hash: TxHashDb,
    pub parent_data_proposal_hash: DataProposalHashDb,
    pub blob_index: i32,
    pub identity: String,
    pub contract_name: String,
    pub blob_data: Vec<u8>,
    pub verified: bool,
}

#[derive(Debug)]
pub struct TxProofStore {
    pub tx_hash: TxHashDb,
    pub parent_data_proposal_hash: DataProposalHashDb,
    pub proof: Vec<u8>,
}

#[derive(Debug)]
pub struct TxEventStore {
    pub block_hash: ConsensusProposalHash,
    pub block_height: i64,
    pub index: i32,
    pub tx_hash: TxHashDb,
    pub parent_data_proposal_hash: DataProposalHashDb,
    pub events: String,
}

#[derive(Debug)]
pub struct TxContractStore {
    pub tx_hash: TxHashDb,
    pub parent_data_proposal_hash: DataProposalHashDb,
    pub verifier: String,
    pub program_id: Vec<u8>,
    pub timeout_window: Option<TimeoutWindowDb>,
    pub state_commitment: Vec<u8>,
    pub contract_name: String,
}

#[derive(Debug)]
pub struct TxContractStateStore {
    pub contract_name: String,
    pub block_hash: ConsensusProposalHash,
    pub state_commitment: Vec<u8>,
}

#[derive(Debug)]
pub struct TxBlobProofOutputStore {
    pub proof_tx_hash: TxHashDb,
    pub proof_parent_dp_hash: DataProposalHashDb,
    pub blob_tx_hash: TxHashDb,
    pub blob_parent_dp_hash: DataProposalHashDb,
    pub blob_index: i32,
    pub blob_proof_output_index: i32,
    pub contract_name: String,
    pub hyle_output: String,
    pub settled: bool,
}

#[derive(Debug)]
pub struct DataProposalStore {
    pub hash: DataProposalHashDb,
    pub parent_hash: Option<DataProposalHashDb>,
    pub lane_id: String,
    pub tx_count: i32,
    pub estimated_size: i64,
    pub block_hash: ConsensusProposalHash,
    pub block_height: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Default)]
pub(crate) struct IndexerHandlerStore {
    blocks: Vec<Arc<Block>>,
    block_txs: HashMap<TxId, (i32, Arc<Block>, Transaction)>,
    tx_data: Vec<TxDataStore>,
    tx_data_proofs: Vec<TxProofStore>,
    transactions_events: Vec<TxEventStore>,
    sql_updates: Vec<
        sqlx::query::Query<
            'static,
            Postgres,
            <sqlx::Postgres as sqlx::Database>::Arguments<'static>,
        >,
    >,
    contracts: HashMap<ContractName, TxContractStore>,
    contract_states: Vec<TxContractStateStore>,
    deleted_contracts: HashSet<ContractName>,
    blob_proof_outputs: Vec<TxBlobProofOutputStore>,
    data_proposals: Vec<DataProposalStore>,
}

impl std::fmt::Debug for IndexerHandlerStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexerHandlerStore")
            .field("blocks", &self.blocks.len())
            .field("block_txs", &self.block_txs.len())
            .field("tx_data", &self.tx_data.len())
            .field("tx_data_proofs", &self.tx_data_proofs.len())
            .field("transactions_events", &self.transactions_events.len())
            .field("sql_updates", &self.sql_updates.len())
            .field("contracts", &self.contracts.len())
            .field("contract_states", &self.contract_states.len())
            .field("blob_proof_outputs", &self.blob_proof_outputs.len())
            .field("data_proposals", &self.data_proposals.len())
            .finish()
    }
}

impl Indexer {
    pub async fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<(), Error> {
        match event {
            NodeStateEvent::NewBlock(block) => self.handle_processed_block(*block)?,
        };

        if self.handler_store.blocks.len() >= self.conf.query_buffer_size {
            // If we have more than configured blocks, we dump the store to the database
            self.dump_store_to_db().await?;
        }
        if let Some(block) = self.handler_store.blocks.last() {
            // if last block is newer than 5sec dump store to db
            let now = TimestampMsClock::now();
            if block.block_timestamp > TimestampMs(now.0 - 5000) {
                self.dump_store_to_db().await?;
            }
        }

        Ok(())
    }

    pub async fn handle_node_state_indexer_event(
        &mut self,
        event: NodeStateIndexerEvent,
    ) -> Result<(), Error> {
        info!("Handling node state indexer event: {:?}", event);
        match event {
            NodeStateIndexerEvent::DataProposalsFromBlock {
                block_hash,
                block_height,
                block_timestamp,
                data_proposals,
            } => {
                self.handle_data_proposals_from_block(
                    block_hash,
                    block_height,
                    block_timestamp,
                    data_proposals,
                )?;
            }
        };

        Ok(())
    }

    pub(crate) async fn dump_store_to_db(&mut self) -> Result<()> {
        if self.handler_store.blocks.is_empty() {
            return Ok(());
        }

        let mut transaction = self.state.db.begin().await?;

        // Insert blocks into the database
        if !self.handler_store.blocks.is_empty() {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO blocks (hash, parent_hash, height, timestamp, total_txs) ",
            );
            query_builder
                .push_values(self.handler_store.blocks.drain(..), |mut b, block| {
                    let block_hash = block.hash.clone();
                    let block_height = log_error!(
                        i64::try_from(block.block_height.0).map_err(|_| anyhow::anyhow!(
                            "Block height is too large to fit into an i64"
                        )),
                        "Converting block height into i64"
                    )
                    .unwrap_or_default();
                    let total_txs = block.txs.len() as i64;

                    let block_timestamp = into_utc_date_time(&block.block_timestamp)
                        .context("Block's timestamp is incorrect")
                        .unwrap_or_else(|_| {
                            // If the timestamp is incorrect, we can use the current time as a fallback.
                            Utc::now()
                        });

                    b.push_bind(block_hash)
                        .push_bind(block.parent_hash.clone())
                        .push_bind(block_height)
                        .push_bind(block_timestamp)
                        .push_bind(total_txs);
                })
                .build()
                .execute(&mut *transaction)
                .await
                .context("Inserting blocks")?;
        }

        // Insert data proposals
        if !self.handler_store.data_proposals.is_empty() {
            let mut query_builder = QueryBuilder::new(
                "INSERT INTO data_proposals (hash, parent_hash, lane_id, tx_count, estimated_size, block_hash, block_height, created_at)",
            );

            query_builder.push_values(&self.handler_store.data_proposals, |mut b, dp| {
                info!(
                    "Inserting data proposal {} with parent hash {:?} in block {}",
                    dp.hash.0,
                    dp.parent_hash.as_ref().map(|h| h.0.clone()),
                    dp.block_hash.0
                );
                b.push_bind(&dp.hash)
                    .push_bind(&dp.parent_hash)
                    .push_bind(&dp.lane_id)
                    .push_bind(dp.tx_count)
                    .push_bind(dp.estimated_size)
                    .push_bind(&dp.block_hash)
                    .push_bind(dp.block_height)
                    .push_bind(dp.created_at);
            });

            query_builder.push(" ON CONFLICT (hash) DO NOTHING");

            query_builder
                .build()
                .execute(&mut *transaction)
                .await
                .context("Inserting data proposals")?;

            self.handler_store.data_proposals.clear();
        }

        // Insert transactions into the database with batching
        if !self.handler_store.block_txs.is_empty() {
            const TRANSACTIONS_PARAMS: usize = 10; // tx_hash, parent_dp_hash, version, transaction_type, transaction_status, block_hash, block_height, index, lane_id, identity
            let transactions_batch_size = calculate_optimal_batch_size(TRANSACTIONS_PARAMS);
            let block_txs = std::mem::take(&mut self.handler_store.block_txs);
            let block_txs_vec: Vec<_> = block_txs.into_iter().collect();
            let chunks: Vec<_> = block_txs_vec.chunks(transactions_batch_size).collect();

            info!(
                "Inserting {} transactions in {} batches of up to {} items each (calculated from {} params per item)",
                block_txs_vec.len(),
                chunks.len(),
                transactions_batch_size,
                TRANSACTIONS_PARAMS
            );

            for (batch_idx, chunk) in chunks.iter().enumerate() {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO transactions (tx_hash, parent_dp_hash, version, transaction_type, transaction_status, block_hash, block_height, index, lane_id, identity) ",
                );
                query_builder.push_values(chunk.iter(), |mut b, (tx_id, (index, block, tx))| {
                    let version = log_error!(
                        i32::try_from(tx.version).map_err(|_| anyhow::anyhow!(
                            "Tx version is too large to fit into an i32"
                        )),
                        "Converting tx version into i32"
                    )
                    .unwrap_or_default();

                    let block_height = log_error!(
                        i64::try_from(block.block_height.0).map_err(|_| anyhow::anyhow!(
                            "Block height is too large to fit into an i64"
                        )),
                        "Converting block height into i64"
                    )
                    .unwrap_or_default();

                    let tx_type = TransactionTypeDb::from(tx);
                    let tx_status = match tx.transaction_data {
                        TransactionData::Blob(_) => TransactionStatusDb::Sequenced,
                        TransactionData::Proof(_) => TransactionStatusDb::Success,
                        TransactionData::VerifiedProof(_) => TransactionStatusDb::Success,
                    };

                    let lane_id: Option<LaneIdDb> = block
                        .lane_ids
                        .get(&tx_id.1)
                        .map(|lane_id| lane_id.clone().into());

                    let identity = match tx.transaction_data {
                        TransactionData::Blob(ref tx) => Some(tx.identity.0.clone()),
                        _ => None,
                    };

                    let parent_data_proposal_hash: DataProposalHashDb = tx_id.0.clone().into();
                    let tx_hash: TxHashDb = tx_id.1.clone().into();

                    b.push_bind(tx_hash)
                        .push_bind(parent_data_proposal_hash)
                        .push_bind(version)
                        .push_bind(tx_type)
                        .push_bind(tx_status)
                        .push_bind(block.hash.clone())
                        .push_bind(block_height)
                        .push_bind(index)
                        .push_bind(lane_id)
                        .push_bind(identity);
                });
                query_builder.push(" ON CONFLICT(tx_hash, parent_dp_hash) DO UPDATE SET ");
                query_builder.push("block_hash=EXCLUDED.block_hash, block_height=EXCLUDED.block_height, index=EXCLUDED.index, lane_id=EXCLUDED.lane_id, identity=EXCLUDED.identity,");
                // This data comes from post-consensus so always erase earlier statuses, but not later ones.
                query_builder.push(
                    "transaction_status = case
                        when transactions.transaction_status IS NULL THEN EXCLUDED.transaction_status
                        when transactions.transaction_status = 'waiting_dissemination' THEN EXCLUDED.transaction_status
                        when transactions.transaction_status = 'data_proposal_created' THEN EXCLUDED.transaction_status
                        else transactions.transaction_status
                    end",
                );

                query_builder
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| {
                        format!(
                            "Inserting transactions batch {} of {}",
                            batch_idx + 1,
                            chunks.len()
                        )
                    })?;
            }
        }

        // Insert blobs into the database with batching
        if !self.handler_store.tx_data.is_empty() {
            const TX_DATA_PARAMS: usize = 7; // tx_hash, parent_dp_hash, blob_index, identity, contract_name, data, verified
            let blob_batch_size = calculate_optimal_batch_size(TX_DATA_PARAMS);

            let tx_data = std::mem::take(&mut self.handler_store.tx_data);
            let chunks: Vec<_> = tx_data.chunks(blob_batch_size).collect();

            info!(
                "Inserting {} blobs in {} batches of up to {} items each (calculated from {} params per item)",
                tx_data.len(),
                chunks.len(),
                blob_batch_size,
                TX_DATA_PARAMS
            );

            for (batch_idx, chunk) in chunks.iter().enumerate() {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO blobs (tx_hash, parent_dp_hash, blob_index, identity, contract_name, data, verified) ",
                );

                query_builder.push_values(chunk.iter(), |mut b, s| {
                    let TxDataStore {
                        tx_hash,
                        parent_data_proposal_hash,
                        blob_index,
                        identity,
                        contract_name,
                        blob_data,
                        verified,
                    } = s;

                    b.push_bind(tx_hash)
                        .push_bind(parent_data_proposal_hash)
                        .push_bind(blob_index)
                        .push_bind(identity)
                        .push_bind(contract_name)
                        .push_bind(blob_data)
                        .push_bind(verified);
                });

                query_builder.push(" ON CONFLICT DO NOTHING");

                query_builder
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| {
                        format!(
                            "Inserting blobs batch {} of {}",
                            batch_idx + 1,
                            chunks.len()
                        )
                    })?;
            }

            // Insert txs_contracts with batching
            const TX_CONTRACTS_PARAMS: usize = 3; // tx_hash, parent_dp_hash, contract_name
            let tx_contracts_batch_size = calculate_optimal_batch_size(TX_CONTRACTS_PARAMS);
            let tx_contracts_chunks: Vec<_> = tx_data.chunks(tx_contracts_batch_size).collect();

            for (batch_idx, chunk) in tx_contracts_chunks.iter().enumerate() {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO txs_contracts (tx_hash, parent_dp_hash, contract_name) ",
                );

                query_builder.push_values(chunk.iter(), |mut b, s| {
                    let TxDataStore {
                        tx_hash,
                        parent_data_proposal_hash,
                        contract_name,
                        ..
                    } = s;

                    b.push_bind(tx_hash)
                        .push_bind(parent_data_proposal_hash)
                        .push_bind(contract_name);
                });

                query_builder.push(" ON CONFLICT DO NOTHING");
                query_builder
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| {
                        format!(
                            "Inserting txs_contracts batch {} of {}",
                            batch_idx + 1,
                            tx_contracts_chunks.len()
                        )
                    })?;
            }
        }

        // Insert proofs into the database with batching
        if !self.handler_store.tx_data_proofs.is_empty() {
            const PROOFS_PARAMS: usize = 3; // parent_dp_hash, tx_hash, proof
            let proofs_batch_size = calculate_optimal_batch_size(PROOFS_PARAMS);
            let tx_data_proofs = std::mem::take(&mut self.handler_store.tx_data_proofs);
            let chunks: Vec<_> = tx_data_proofs.chunks(proofs_batch_size).collect();

            info!(
                "Inserting {} proofs in {} batches of up to {} items each (calculated from {} params per item)",
                tx_data_proofs.len(),
                chunks.len(),
                proofs_batch_size,
                PROOFS_PARAMS
            );

            for (batch_idx, chunk) in chunks.iter().enumerate() {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO proofs (parent_dp_hash, tx_hash, proof) ",
                );

                query_builder.push_values(chunk.iter(), |mut b, s| {
                    let TxProofStore {
                        tx_hash,
                        parent_data_proposal_hash,
                        proof,
                    } = s;

                    b.push_bind(parent_data_proposal_hash)
                        .push_bind(tx_hash)
                        .push_bind(proof);
                });

                query_builder.push(" ON CONFLICT(parent_dp_hash, tx_hash) DO NOTHING");

                query_builder
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| {
                        format!(
                            "Inserting proofs batch {} of {}",
                            batch_idx + 1,
                            chunks.len()
                        )
                    })?;
            }
        }

        // Insert transaction events into the database with batching
        if !self.handler_store.transactions_events.is_empty() {
            const TX_EVENTS_PARAMS: usize = 6; // block_hash, block_height, index, tx_hash, parent_dp_hash, events
            let tx_events_batch_size = calculate_optimal_batch_size(TX_EVENTS_PARAMS);
            let transactions_events = std::mem::take(&mut self.handler_store.transactions_events);
            let chunks: Vec<_> = transactions_events.chunks(tx_events_batch_size).collect();

            info!(
                "Inserting {} transaction events in {} batches of up to {} items each (calculated from {} params per item)",
                transactions_events.len(),
                chunks.len(),
                tx_events_batch_size,
                TX_EVENTS_PARAMS
            );

            for (batch_idx, chunk) in chunks.iter().enumerate() {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO transaction_state_events (block_hash, block_height, index, tx_hash, parent_dp_hash, events) ",
                );
                query_builder.push_values(chunk.iter(), |mut b, s| {
                    let TxEventStore {
                        block_hash,
                        block_height,
                        index,
                        tx_hash,
                        parent_data_proposal_hash,
                        events,
                    } = s;

                    b.push_bind(block_hash)
                        .push_bind(block_height)
                        .push_bind(index)
                        .push_bind(tx_hash)
                        .push_bind(parent_data_proposal_hash)
                        .push_bind(events)
                        .push_unseparated("::jsonb");
                });

                query_builder
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| {
                        format!(
                            "Inserting transaction events batch {} of {}",
                            batch_idx + 1,
                            chunks.len()
                        )
                    })?;
            }
        }

        // Insert contracts into the database with batching
        if !self.handler_store.contracts.is_empty() {
            const CONTRACTS_PARAMS: usize = 6; // tx_hash, parent_dp_hash, verifier, program_id, state_commitment, contract_name
            let contracts_batch_size = calculate_optimal_batch_size(CONTRACTS_PARAMS);
            let contracts = std::mem::take(&mut self.handler_store.contracts);
            let contracts_vec: Vec<_> = contracts.into_values().collect();
            let chunks: Vec<_> = contracts_vec.chunks(contracts_batch_size).collect();

            info!(
                "Inserting {} contracts in {} batches of up to {} items each (calculated from {} params per item)",
                contracts_vec.len(),
                chunks.len(),
                contracts_batch_size,
                CONTRACTS_PARAMS
            );

            for (batch_idx, chunk) in chunks.iter().enumerate() {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO contracts (tx_hash, parent_dp_hash, verifier, program_id, timeout_window, state_commitment, contract_name) ",
                );

                query_builder.push_values(chunk.iter(), |mut b, s| {
                    let TxContractStore {
                        tx_hash,
                        parent_data_proposal_hash,
                        verifier,
                        program_id,
                        timeout_window,
                        state_commitment,
                        contract_name,
                    } = s;

                    b.push_bind(tx_hash)
                        .push_bind(parent_data_proposal_hash)
                        .push_bind(verifier)
                        .push_bind(program_id)
                        .push_bind(timeout_window)
                        .push_bind(state_commitment)
                        .push_bind(contract_name);
                });

                query_builder.push(" ON CONFLICT (contract_name) DO UPDATE SET ");
                query_builder.push("tx_hash = EXCLUDED.tx_hash, ");
                query_builder.push("parent_dp_hash = EXCLUDED.parent_dp_hash, ");
                query_builder.push("verifier = EXCLUDED.verifier, ");
                query_builder.push("program_id = EXCLUDED.program_id, ");
                query_builder.push("timeout_window = EXCLUDED.timeout_window, ");
                query_builder.push("state_commitment = EXCLUDED.state_commitment ");

                query_builder
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| {
                        format!(
                            "Inserting contracts batch {} of {}",
                            batch_idx + 1,
                            chunks.len()
                        )
                    })?;
            }
        }

        // Insert contract states into the database with batching
        if !self.handler_store.contract_states.is_empty() {
            const CONTRACT_STATES_PARAMS: usize = 3; // contract_name, block_hash, state_commitment
            let contract_states_batch_size = calculate_optimal_batch_size(CONTRACT_STATES_PARAMS);
            let contract_states = std::mem::take(&mut self.handler_store.contract_states);
            let chunks: Vec<_> = contract_states.chunks(contract_states_batch_size).collect();

            info!(
                "Inserting {} contract states in {} batches of up to {} items each (calculated from {} params per item)",
                contract_states.len(),
                chunks.len(),
                contract_states_batch_size,
                CONTRACT_STATES_PARAMS
            );

            for (batch_idx, chunk) in chunks.iter().enumerate() {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO contract_state (contract_name, block_hash, state_commitment) ",
                );

                query_builder.push_values(chunk.iter(), |mut b, s| {
                    let TxContractStateStore {
                        contract_name,
                        block_hash,
                        state_commitment,
                    } = s;

                    b.push_bind(contract_name)
                        .push_bind(block_hash)
                        .push_bind(state_commitment);
                });

                query_builder
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| {
                        format!(
                            "Inserting contract states batch {} of {}",
                            batch_idx + 1,
                            chunks.len()
                        )
                    })?;
            }
        }

        // Then delete contracts that were deleted (slightly inefficient but we don't expect many deletions)
        for contract_name in self.handler_store.deleted_contracts.drain() {
            sqlx::query("DELETE FROM contracts WHERE contract_name = $1")
                .bind(contract_name.0)
                .execute(&mut *transaction)
                .await
                .context("Deleting contracts")?;
        }

        // Insert blob proof outputs into the database with batching
        if !self.handler_store.blob_proof_outputs.is_empty() {
            const BLOB_PROOF_OUTPUT_PARAMS: usize = 9; // proof_tx_hash, proof_parent_dp_hash, blob_tx_hash, blob_parent_dp_hash, blob_index, blob_proof_output_index, contract_name, hyle_output, settled
            let blob_proof_outputs_batch_size =
                calculate_optimal_batch_size(BLOB_PROOF_OUTPUT_PARAMS);

            let blob_proof_outputs = std::mem::take(&mut self.handler_store.blob_proof_outputs);
            let chunks: Vec<_> = blob_proof_outputs
                .chunks(blob_proof_outputs_batch_size)
                .collect();

            info!(
                "Inserting {} blob proof outputs in {} batches of up to {} items each (calculated from {} params per item)",
                blob_proof_outputs.len(),
                chunks.len(),
                blob_proof_outputs_batch_size,
                BLOB_PROOF_OUTPUT_PARAMS
            );

            for (batch_idx, chunk) in chunks.iter().enumerate() {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO blob_proof_outputs (proof_tx_hash, proof_parent_dp_hash, blob_tx_hash, blob_parent_dp_hash, blob_index, blob_proof_output_index, contract_name, hyle_output, settled) ",
                );

                query_builder.push_values(chunk.iter(), |mut b, s| {
                    let TxBlobProofOutputStore {
                        proof_tx_hash,
                        proof_parent_dp_hash,
                        blob_tx_hash,
                        blob_parent_dp_hash,
                        blob_index,
                        blob_proof_output_index,
                        contract_name,
                        hyle_output,
                        settled,
                    } = s;

                    b.push_bind(proof_tx_hash)
                        .push_bind(proof_parent_dp_hash)
                        .push_bind(blob_tx_hash)
                        .push_bind(blob_parent_dp_hash)
                        .push_bind(blob_index)
                        .push_bind(blob_proof_output_index)
                        .push_bind(contract_name)
                        .push_bind(hyle_output)
                        .push_unseparated("::jsonb")
                        .push_bind(settled);
                });

                query_builder
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .with_context(|| {
                        format!(
                            "Inserting blob proof outputs batch {} of {}",
                            batch_idx + 1,
                            chunks.len()
                        )
                    })?;
            }
        }

        if !self.handler_store.sql_updates.is_empty() {
            for sql_update in self.handler_store.sql_updates.drain(..) {
                sql_update
                    .execute(&mut *transaction)
                    .await
                    .context("Executing SQL update")?;
            }
        }

        transaction.commit().await?;

        Ok(())
    }

    pub async fn handle_mempool_status_event(&mut self, event: MempoolStatusEvent) -> Result<()> {
        let mut transaction = self.state.db.begin().await?;
        match event {
            MempoolStatusEvent::WaitingDissemination {
                parent_data_proposal_hash,
                tx,
            } => {
                let parent_data_proposal_hash_db: DataProposalHashDb =
                    parent_data_proposal_hash.into();
                let tx_hash: TxHash = tx.hashed();
                let version = i32::try_from(tx.version)
                    .map_err(|_| anyhow::anyhow!("Tx version is too large to fit into an i32"))?;

                // Insert the transaction into the transactions table
                let tx_type = TransactionTypeDb::from(&tx);
                let tx_hash: &TxHashDb = &tx_hash.into();

                info!(
                    "Inserting waiting_dissemination TX {} with parent data proposal hash {}",
                    tx_hash.0, parent_data_proposal_hash_db.0
                );

                // If the TX is already present, we can assume it's more up-to-date so do nothing.
                sqlx::query(
                    "INSERT INTO transactions (tx_hash, parent_dp_hash, version, transaction_type, transaction_status)
                    VALUES ($1, $2, $3, $4, 'waiting_dissemination')
                    ON CONFLICT(tx_hash, parent_dp_hash) DO NOTHING",
                )
                    .bind(tx_hash)
                    .bind(parent_data_proposal_hash_db.clone())
                    .bind(version)
                    .bind(tx_type)
                    .execute(&mut *transaction)
                    .await?;

                _ = log_warn!(
                    self.insert_tx_data(tx_hash, &tx, parent_data_proposal_hash_db,),
                    "Inserting tx data at status 'waiting dissemination'"
                );
            }

            MempoolStatusEvent::DataProposalCreated {
                parent_data_proposal_hash,
                data_proposal_hash,
                txs_metadatas,
            } => {
                let mut seen = HashSet::new();
                let unique_txs_metadatas: Vec<_> = txs_metadatas
                    .into_iter()
                    .filter(|value| {
                        let key = (value.id.1.clone(), value.id.0.clone());
                        seen.insert(key)
                    })
                    .collect();

                let mut query_builder = QueryBuilder::new(
                    "INSERT INTO transactions (tx_hash, parent_dp_hash, version, transaction_type, transaction_status)",
                );

                query_builder.push_values(unique_txs_metadatas, |mut b, value| {
                    let tx_type: TransactionTypeDb = value.transaction_kind.into();
                    let version = log_error!(
                        i32::try_from(value.version).map_err(|_| anyhow::anyhow!(
                            "Tx version is too large to fit into an i32"
                        )),
                        "Converting version number into i32"
                    )
                    .unwrap_or(0);

                    let tx_hash: TxHashDb = value.id.1.into();
                    let parent_data_proposal_hash_db: DataProposalHashDb = value.id.0.into();

                    info!(
                        "Inserting data_proposal_created TX {} with parent data proposal hash {}",
                        tx_hash.0, parent_data_proposal_hash_db.0
                    );

                    b.push_bind(tx_hash)
                        .push_bind(parent_data_proposal_hash_db)
                        .push_bind(version)
                        .push_bind(tx_type)
                        .push_bind(TransactionStatusDb::DataProposalCreated);
                });

                // If the TX is already present, we try to update its status, only if the status is lower ('waiting_dissemination').
                query_builder.push(" ON CONFLICT(tx_hash, parent_dp_hash) DO UPDATE SET ");

                query_builder.push("transaction_status=");
                query_builder.push_bind(TransactionStatusDb::DataProposalCreated);
                query_builder
                    .push(" WHERE transactions.transaction_status='waiting_dissemination'");

                query_builder
                    .build()
                    .execute(transaction.deref_mut())
                    .await
                    .context("Upserting data at status data_proposal_created")?;

                // Second step - any TX that was skipped for this DP needs to have its ID updated
                // (this should be all TXs with the same parent as us still in waiting dissemination).
                let mut query_builder =
                    QueryBuilder::new("UPDATE transactions SET parent_dp_hash = ");
                let parent_data_proposal_hash_db: DataProposalHashDb =
                    parent_data_proposal_hash.clone().into();
                let data_proposal_hash_db: DataProposalHashDb = data_proposal_hash.clone().into();
                query_builder
                    .push_bind(data_proposal_hash_db.clone())
                    .push(" WHERE parent_dp_hash = ")
                    .push_bind(parent_data_proposal_hash_db)
                    .push(" AND transaction_status = 'waiting_dissemination' RETURNING tx_hash");
                let txs = query_builder
                    .build()
                    .fetch_all(transaction.deref_mut())
                    .await
                    .context("Updating parent data proposal hash")?;
                // Then we need to update blobs
                let tx_hashes = txs
                    .iter()
                    .filter_map(|row| {
                        if let Ok(tx_hash) = row.try_get::<TxHashDb, _>(0) {
                            self.handler_store.tx_data.iter_mut().for_each(|tx_data| {
                                if tx_data.tx_hash == tx_hash
                                    && tx_data.parent_data_proposal_hash.0
                                        == parent_data_proposal_hash
                                {
                                    tx_data.parent_data_proposal_hash =
                                        data_proposal_hash_db.clone();
                                }
                            });
                            return Some(tx_hash);
                        }
                        None
                    })
                    .collect::<Vec<_>>();
                if !tx_hashes.is_empty() {
                    let mut query_builder = QueryBuilder::new("UPDATE blobs SET parent_dp_hash = ");
                    let parent_data_proposal_hash_db: DataProposalHashDb =
                        parent_data_proposal_hash.into();
                    let data_proposal_hash_db: DataProposalHashDb = data_proposal_hash.into();

                    info!(
                        "Updating skipped TXs with parent data proposal hash {} to new DP hash {}: {:?}",
                        parent_data_proposal_hash_db.0, data_proposal_hash_db.0, tx_hashes
                    );

                    query_builder
                        .push_bind(data_proposal_hash_db.clone())
                        .push(" WHERE parent_dp_hash = ")
                        .push_bind(parent_data_proposal_hash_db)
                        .push(" AND tx_hash in ");
                    query_builder.push_tuples(tx_hashes, |mut b, tx_hash| {
                        b.push_bind(tx_hash);
                    });
                    query_builder
                        .build()
                        .execute(transaction.deref_mut())
                        .await
                        .context("Updating parent data proposal hash")?;
                }
            }
        }

        transaction.commit().await?;

        Ok(())
    }

    fn insert_tx_data(
        &mut self,
        tx_hash: &TxHashDb,
        tx: &Transaction,
        parent_data_proposal_hash: DataProposalHashDb,
    ) -> Result<()> {
        match &tx.transaction_data {
            TransactionData::Blob(blob_tx) => {
                blob_tx
                    .blobs
                    .iter()
                    .enumerate()
                    .for_each(|(blob_index, blob)| {
                        let blob_index = log_error!(
                            i32::try_from(blob_index),
                            "Blob index is too large to fit into an i32"
                        )
                        .unwrap_or_default();

                        let identity = blob_tx.identity.0.clone();
                        let contract_name = blob.contract_name.0.clone();
                        let blob_data = blob.data.0.clone();
                        let tx_hash = tx_hash.clone();
                        let parent_data_proposal_hash = parent_data_proposal_hash.clone();

                        self.handler_store.tx_data.push(TxDataStore {
                            tx_hash,
                            parent_data_proposal_hash,
                            blob_index,
                            identity,
                            contract_name,
                            blob_data,
                            verified: false,
                        });
                    });
            }
            TransactionData::VerifiedProof(tx_data) => {
                // Then insert the proof in to the proof table.
                match &tx_data.proof {
                    Some(proof_data) => {
                        self.handler_store.tx_data_proofs.push(TxProofStore {
                            tx_hash: tx_hash.clone(),
                            parent_data_proposal_hash: parent_data_proposal_hash.clone(),
                            proof: proof_data.0.clone(),
                        });
                    }
                    None => {
                        tracing::trace!(
                            "Verified proof TX {:?} does not contain a proof",
                            &tx_hash
                        );
                    }
                };
            }
            _ => {
                bail!("Unsupported transaction type");
            }
        }
        Ok(())
    }

    pub(crate) fn handle_processed_block(&mut self, block: Block) -> Result<(), Error> {
        if block.block_height.0 % 1000 == 0 {
            // Log every 1000th block
            info!("Indexing block at height {:?}", block.block_height);
        } else {
            trace!("Indexing block at height {:?}", block.block_height);
        }

        let arc_block = Arc::new(block.clone());

        self.handler_store.blocks.push(arc_block.clone());

        let block_height = i64::try_from(block.block_height.0)
            .map_err(|_| anyhow::anyhow!("Block height is too large to fit into an i64"))?;

        let mut i: i32 = 0;
        #[allow(clippy::explicit_counter_loop)]
        for (tx_id, tx) in &block.txs {
            info!(
                "Processing transaction {} at block height {} in data proposal {:?}",
                tx_id,
                block_height,
                block.dp_parent_hashes.get(&tx_id.1).context(format!(
                    "No parent data proposal hash present for tx {}",
                    &tx_id.1
                ))
            );

            let tx_hash: TxHashDb = tx_id.1.clone().into();
            let ctx = block.build_tx_ctx(&tx_hash.0);
            let (lane_id, timestamp) = match ctx {
                Ok(ctx) => (Some(ctx.lane_id), Some(ctx.timestamp)),
                Err(_) => (None, None),
            };

            self.handler_store
                .block_txs
                .insert(tx_id.clone(), (i, arc_block.clone(), tx.clone()));

            let parent_data_proposal_hash: DataProposalHashDb = tx_id.0.clone().into();

            _ = log_warn!(
                self.insert_tx_data(&tx_hash, tx, parent_data_proposal_hash.clone(),),
                "Inserting tx data when tx in block"
            );

            if let TransactionData::Blob(blob_tx) = &tx.transaction_data {
                // Send the transaction to all websocket subscribers
                self.send_blob_transaction_to_websocket_subscribers(
                    blob_tx,
                    tx_hash,
                    parent_data_proposal_hash,
                    &block.hash,
                    i as u32,
                    tx.version,
                    lane_id,
                    timestamp,
                );
            }

            i += 1;
        }

        for (i, (tx_hash, events)) in (0..).zip(block.transactions_events.into_iter()) {
            let tx_hash_db: &TxHashDb = &tx_hash.clone().into();
            let parent_data_proposal_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&tx_hash)
                .context(format!(
                    "No parent data proposal hash present for tx {}",
                    &tx_hash
                ))?
                .clone()
                .into();
            let serialized_events = serde_json::to_string(&events)?;
            debug!("Inserting transaction state event {tx_hash}: {serialized_events}");

            self.handler_store.transactions_events.push(TxEventStore {
                block_hash: block.hash.clone(),
                block_height,
                index: i,
                tx_hash: tx_hash_db.clone(),
                parent_data_proposal_hash,
                events: serialized_events,
            });
        }

        // Handling new stakers
        for _staker in block.staking_actions {
            // TODO: add new table with stakers at a given height
        }

        // Handling settled blob transactions
        for settled_blob_tx_hash in block.successful_txs {
            let dp_hash_db: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&settled_blob_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for settled blob tx {}",
                    settled_blob_tx_hash.0
                ))?
                .clone()
                .into();
            let tx_hash: TxHashDb = settled_blob_tx_hash.into();
            self.handler_store.sql_updates.push(
                sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                    .bind(TransactionStatusDb::Success)
                    .bind(tx_hash)
                    .bind(dp_hash_db)
            );
        }

        for failed_blob_tx_hash in block.failed_txs {
            let dp_hash_db: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&failed_blob_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for failed blob tx {}",
                    failed_blob_tx_hash.0
                ))?
                .clone()
                .into();
            let tx_hash: TxHashDb = failed_blob_tx_hash.into();
            self.handler_store.sql_updates.push(
                sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                    .bind(TransactionStatusDb::Failure)
                    .bind(tx_hash)
                    .bind(dp_hash_db)
            );
        }

        // Handling timed out blob transactions
        for timed_out_tx_hash in block.timed_out_txs {
            let dp_hash_db: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&timed_out_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for timed out tx {}",
                    timed_out_tx_hash.0
                ))?
                .clone()
                .into();
            let tx_hash: TxHashDb = timed_out_tx_hash.into();
            self.handler_store.sql_updates.push(
                sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                    .bind(TransactionStatusDb::TimedOut)
                    .bind(tx_hash)
                    .bind(dp_hash_db)
            );
        }

        for handled_blob_proof_output in block.blob_proof_outputs {
            let proof_dp_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&handled_blob_proof_output.proof_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for proof tx {}",
                    handled_blob_proof_output.proof_tx_hash.0
                ))?
                .clone()
                .into();
            let blob_dp_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&handled_blob_proof_output.blob_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for blob tx {}",
                    handled_blob_proof_output.blob_tx_hash.0
                ))?
                .clone()
                .into();
            let proof_tx_hash: &TxHashDb = &handled_blob_proof_output.proof_tx_hash.into();
            let blob_tx_hash: &TxHashDb = &handled_blob_proof_output.blob_tx_hash.into();
            let blob_index = i32::try_from(handled_blob_proof_output.blob_index.0)
                .map_err(|_| anyhow::anyhow!("Blob index is too large to fit into an i32"))?;
            let blob_proof_output_index =
                i32::try_from(handled_blob_proof_output.blob_proof_output_index).map_err(|_| {
                    anyhow::anyhow!("Blob proof output index is too large to fit into an i32")
                })?;
            let serialized_hyle_output =
                serde_json::to_string(&handled_blob_proof_output.hyle_output)?;

            self.handler_store
                .blob_proof_outputs
                .push(TxBlobProofOutputStore {
                    proof_tx_hash: proof_tx_hash.clone(),
                    proof_parent_dp_hash: proof_dp_hash.clone(),
                    blob_tx_hash: blob_tx_hash.clone(),
                    blob_parent_dp_hash: blob_dp_hash.clone(),
                    blob_index,
                    blob_proof_output_index,
                    contract_name: handled_blob_proof_output.contract_name.0.clone(),
                    hyle_output: serialized_hyle_output,
                    settled: false,
                });
        }

        // Handling verified blob (! must come after blob proof output, as it updates that)
        for (blob_tx_hash, blob_index, blob_proof_output_index) in block.verified_blobs {
            let blob_tx_parent_dp_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&blob_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for verified blob tx {}",
                    blob_tx_hash.0
                ))?
                .clone()
                .into();
            let blob_tx_hash: TxHashDb = blob_tx_hash.into();
            let blob_index = i32::try_from(blob_index.0)
                .map_err(|_| anyhow::anyhow!("Blob index is too large to fit into an i32"))?;

            self.handler_store.sql_updates.push(
                sqlx::query("UPDATE blobs SET verified = true WHERE tx_hash = $1 AND parent_dp_hash = $2 AND blob_index = $3")
                    .bind(blob_tx_hash.clone())
                    .bind(blob_tx_parent_dp_hash.clone())
                    .bind(blob_index)
            );

            if let Some(blob_proof_output_index) = blob_proof_output_index {
                let blob_proof_output_index =
                    i32::try_from(blob_proof_output_index).map_err(|_| {
                        anyhow::anyhow!("Blob proof output index is too large to fit into an i32")
                    })?;

                self.handler_store.sql_updates.push(
                    sqlx::query("UPDATE blob_proof_outputs SET settled = true WHERE blob_tx_hash = $1 AND blob_parent_dp_hash = $2 AND blob_index = $3 AND blob_proof_output_index = $4")
                        .bind(blob_tx_hash)
                        .bind(blob_tx_parent_dp_hash)
                        .bind(blob_index)
                        .bind(blob_proof_output_index)
                );
            }
        }

        // After TXes as it refers to those (for now)
        for (tx_hash, contract, _) in block.registered_contracts.values() {
            let verifier = &contract.verifier.0;
            let program_id = &contract.program_id.0;
            let state_commitment = &contract.state_commitment.0;
            let contract_name = &contract.contract_name.0;
            let tx_parent_dp_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(tx_hash)
                .context(format!(
                    "No parent data proposal hash present for registered contract tx {}",
                    tx_hash.0
                ))?
                .clone()
                .into();
            let tx_hash: &TxHashDb = &tx_hash.clone().into();

            // If this had previously been deleted, clear that.
            self.handler_store
                .deleted_contracts
                .remove(&contract.contract_name);

            // Adding to Contract table
            self.handler_store.contracts.insert(
                contract.contract_name.clone(),
                TxContractStore {
                    tx_hash: tx_hash.clone(),
                    parent_data_proposal_hash: tx_parent_dp_hash.clone(),
                    verifier: verifier.clone(),
                    program_id: program_id.clone(),
                    timeout_window: contract.timeout_window.clone().map(|tw| tw.into()),
                    state_commitment: state_commitment.clone(),
                    contract_name: contract_name.clone(),
                },
            );

            // Adding to ContractState table
            self.handler_store
                .contract_states
                .push(TxContractStateStore {
                    contract_name: contract_name.clone(),
                    block_hash: block.hash.clone(),
                    state_commitment: state_commitment.clone(),
                });
        }

        for contract_name in block.deleted_contracts.keys() {
            self.handler_store
                .deleted_contracts
                .insert(contract_name.clone());
        }

        // Handling updated contract state
        for (contract_name, state_commitment) in block.updated_states {
            let contract_name = contract_name.0;
            let state_commitment = state_commitment.0;
            self.handler_store.sql_updates.push(
                sqlx::query(
                    "UPDATE contract_state SET state_commitment = $1 WHERE contract_name = $2 AND block_hash = $3",
                )
                    .bind(state_commitment.clone())
                    .bind(contract_name.clone())
                    .bind(block.hash.clone())
            );

            self.handler_store.sql_updates.push(
                sqlx::query::<Postgres>(
                    "UPDATE contracts SET state_commitment = $1 WHERE contract_name = $2",
                )
                .bind(state_commitment)
                .bind(contract_name),
            );
        }

        // Handling updated contract program ids
        for (contract_name, program_id) in block.updated_program_ids {
            let contract_name = contract_name.0;
            let program_id = program_id.0;

            self.handler_store.sql_updates.push(
                sqlx::query::<Postgres>(
                    "UPDATE contracts SET program_id = $1 WHERE contract_name = $2",
                )
                .bind(program_id)
                .bind(contract_name),
            );
        }

        // Handling updated contract program ids
        for (contract_name, timeout_window) in block.updated_timeout_windows {
            let contract_name = contract_name.0;

            let timeout_window_db: TimeoutWindowDb = timeout_window.into();
            self.handler_store.sql_updates.push(
                sqlx::query::<Postgres>(
                    "UPDATE contracts SET timeout_window = $1 WHERE contract_name = $2",
                )
                .bind(timeout_window_db)
                .bind(contract_name),
            );
        }

        Ok(())
    }

    fn handle_data_proposals_from_block(
        &mut self,
        block_hash: ConsensusProposalHash,
        block_height: BlockHeight,
        block_timestamp: TimestampMs,
        data_proposals: Vec<DataProposalMetadata>,
    ) -> Result<(), Error> {
        let block_height_i64 = i64::try_from(block_height.0)
            .map_err(|_| anyhow::anyhow!("Block height too large for i64"))?;

        for dp_metadata in data_proposals {
            // A bit hacky, but it allows to have no empty field in dp_hash for first lane's dp
            // transactions
            let parent_dp = dp_metadata
                .parent_hash
                .clone()
                .map(|h| h.into())
                .unwrap_or_else(|| {
                    DataProposalHashDb(DataProposalHash(hex::encode(&dp_metadata.lane_id.0 .0)))
                });

            // If this is the first DP in a lane (no parent_hash), ensure the virtual DP exists
            if dp_metadata.parent_hash.as_ref().map(|h| &h.0)
                == Some(&hex::encode(&dp_metadata.lane_id.0 .0))
            {
                let lane_id_str = hex::encode(&dp_metadata.lane_id.0 .0);
                let lane_id_dp_hash: DataProposalHashDb =
                    DataProposalHash(lane_id_str.clone()).into();

                if !self
                    .handler_store
                    .data_proposals
                    .iter()
                    .any(|dp| dp.hash == lane_id_dp_hash)
                {
                    self.handler_store.data_proposals.push(DataProposalStore {
                        hash: lane_id_dp_hash,
                        parent_hash: None,
                        lane_id: lane_id_str,
                        tx_count: 0, // Virtual DP, no actual transactions
                        estimated_size: 0,
                        block_hash: block_hash.clone(),
                        block_height: block_height_i64,
                        created_at: into_utc_date_time(&block_timestamp)?,
                    });
                }
            }

            let dp_store = DataProposalStore {
                hash: dp_metadata.hash.into(),
                parent_hash: Some(parent_dp),
                lane_id: hex::encode(&dp_metadata.lane_id.0 .0),
                tx_count: dp_metadata.tx_count as i32,
                estimated_size: dp_metadata.estimated_size as i64,
                block_hash: block_hash.clone(),
                block_height: block_height_i64,
                created_at: into_utc_date_time(&block_timestamp)?,
            };

            for tx in &dp_metadata.tx_hashes {
                let tx_hash: TxHashDb = tx.clone().into();
                self.handler_store.sql_updates.push(
                    sqlx::query("UPDATE transactions SET dp_hash = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                        .bind(dp_store.hash.clone())
                        .bind(tx_hash)
                        .bind(dp_store.parent_hash.clone())
                );
            }
            self.handler_store.data_proposals.push(dp_store);
        }

        Ok(())
    }
}

pub fn into_utc_date_time(ts: &TimestampMs) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ts.0.try_into().context("Converting u64 into i64")?)
        .context("Converting i64 into UTC DateTime")
}
