//! Index system for historical data.

mod handler;

use crate::explorer::api::{DataProposalHashDb, TxHashDb};
use crate::explorer::WsExplorerBlobTx;
use crate::node_state::module::NodeStateEvent;
use crate::{model::*, utils::conf::SharedConf};
use anyhow::{Context, Result};
use handler::IndexerHandlerStore;
use hyle_model::utils::TimestampMs;
use hyle_modules::bus::BusClientSender;
use hyle_modules::{
    bus::SharedMessageBus,
    log_error, module_handle_messages,
    modules::{module_bus_client, Module, SharedBuildApiCtx},
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::{postgres::PgPoolOptions, PgPool, Pool, Postgres};

module_bus_client! {
#[derive(Debug)]
struct IndexerBusClient {
    sender(WsExplorerBlobTx),
    receiver(NodeStateEvent),
    receiver(MempoolStatusEvent),
}
}

#[derive(Debug)]
pub struct Indexer {
    bus: IndexerBusClient,
    db: PgPool,
    handler_store: IndexerHandlerStore,
    conf: IndexerConf,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct IndexerConf {
    query_buffer_size: usize,
}

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./src/indexer/migrations");

impl Module for Indexer {
    type Context = (SharedConf, SharedBuildApiCtx);

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = IndexerBusClient::new_from_bus(bus.new_handle()).await;

        let pool = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect(&ctx.0.database_url)
            .await
            .context("Failed to connect to the database")?;

        tokio::time::timeout(tokio::time::Duration::from_secs(60), MIGRATOR.run(&pool)).await??;

        let indexer = Indexer {
            bus,
            db: pool,
            handler_store: IndexerHandlerStore::default(),
            conf: ctx.0.indexer.clone(),
        };

        Ok(indexer)
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }
}

impl Indexer {
    pub async fn start(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<NodeStateEvent> event => {
                _ = log_error!(self.handle_node_state_event(event)
                    .await,
                    "Indexer handling node state event");
            }

            listen<MempoolStatusEvent> _event => {
                // _ = log_error!(self.handle_mempool_status_event(event)
                //     .await,
                //     "Indexer handling mempool status event");
            }

        };
        Ok(())
    }

    pub async fn get_last_block(&self) -> Result<Option<BlockHeight>> {
        let rows = sqlx::query("SELECT max(height) as max FROM blocks")
            .fetch_one(&self.db)
            .await?;
        Ok(rows
            .try_get("max")
            .map(|m: i64| Some(BlockHeight(m as u64)))
            .unwrap_or(None))
    }

    #[allow(clippy::too_many_arguments)]
    fn send_blob_transaction_to_websocket_subscribers(
        &mut self,
        tx: &BlobTransaction,
        tx_hash: TxHashDb,
        dp_hash: DataProposalHashDb,
        block_hash: &ConsensusProposalHash,
        index: u32,
        version: u32,
        lane_id: Option<LaneId>,
        timestamp: Option<TimestampMs>,
    ) {
        let _ = self.bus.send(WsExplorerBlobTx {
            tx: tx.clone(),
            tx_hash,
            dp_hash,
            block_hash: block_hash.clone(),
            index,
            version,
            lane_id,
            timestamp,
        });
    }
}

impl std::ops::Deref for Indexer {
    type Target = Pool<Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

#[cfg(test)]
mod test {
    use assert_json_diff::assert_json_include;
    use axum_test::TestServer;
    use client_sdk::transaction_builder::ProvableBlobTx;
    use hydentity::{client::tx_executor_handler::register_identity, HydentityAction};
    use hyle_contract_sdk::{BlobIndex, HyleOutput, Identity, ProgramId, StateCommitment, TxHash};
    use hyle_model::api::{
        APIBlob, APIBlock, APIContract, APITransaction, APITransactionEvents, TransactionStatusDb,
    };
    use serde_json::json;
    use std::future::IntoFuture;
    use utils::TimestampMs;

    use crate::{
        bus::SharedMessageBus,
        explorer::Explorer,
        model::{
            Blob, BlobData, BlobProofOutput, ProofData, SignedBlock, Transaction, TransactionData,
            VerifiedProofTransaction,
        },
        node_state::{metrics::NodeStateMetrics, NodeState, NodeStateStore},
    };

    use super::*;

    use sqlx::postgres::PgPoolOptions;
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
    };

    async fn setup_test_server(explorer: &Explorer) -> Result<TestServer> {
        let router = explorer.api(None);
        TestServer::new(router)
    }

    async fn new_indexer(pool: PgPool) -> (Indexer, Explorer) {
        let bus = SharedMessageBus::default();
        (
            Indexer {
                bus: IndexerBusClient::new_from_bus(bus.new_handle()).await,
                db: pool.clone(),
                handler_store: IndexerHandlerStore::default(),
                conf: IndexerConf {
                    query_buffer_size: 100,
                },
            },
            Explorer::new(bus, pool).await,
        )
    }

    fn new_register_tx(
        contract_name: ContractName,
        state_commitment: StateCommitment,
    ) -> BlobTransaction {
        BlobTransaction::new(
            "hyle@hyle",
            vec![RegisterContractAction {
                verifier: "test".into(),
                program_id: ProgramId(vec![3, 2, 1]),
                state_commitment,
                contract_name,
                ..Default::default()
            }
            .as_blob("hyle".into(), None, None)],
        )
    }

    pub fn new_delete_tx(tld: ContractName, contract_name: ContractName) -> BlobTransaction {
        BlobTransaction::new(
            "hyli@wallet".to_string(),
            vec![
                HydentityAction::VerifyIdentity {
                    nonce: 0,
                    account: "hyli@wallet".to_string(),
                }
                .as_blob("wallet".into()),
                DeleteContractAction { contract_name }.as_blob(tld, None, None),
            ],
        )
    }

    fn new_blob_tx(
        identity: Identity,
        first_contract_name: ContractName,
        second_contract_name: ContractName,
    ) -> Transaction {
        Transaction {
            version: 1,
            transaction_data: TransactionData::Blob(BlobTransaction::new(
                identity,
                vec![
                    Blob {
                        contract_name: first_contract_name,
                        data: BlobData(vec![1, 2, 3]),
                    },
                    Blob {
                        contract_name: second_contract_name,
                        data: BlobData(vec![1, 2, 3]),
                    },
                ],
            )),
        }
    }

    fn new_proof_tx(
        identity: Identity,
        contract_name: ContractName,
        blob_index: BlobIndex,
        blob_transaction: &Transaction,
        initial_state: StateCommitment,
        next_state: StateCommitment,
    ) -> Transaction {
        let TransactionData::Blob(blob_tx) = &blob_transaction.transaction_data else {
            panic!("Expected BlobTransaction");
        };
        let proof = ProofData(initial_state.0.clone());
        Transaction {
            version: 1,
            transaction_data: TransactionData::VerifiedProof(VerifiedProofTransaction {
                contract_name: contract_name.clone(),
                proof_hash: proof.hashed(),
                proof_size: proof.0.len(),
                proven_blobs: vec![BlobProofOutput {
                    original_proof_hash: proof.hashed(),
                    program_id: ProgramId(vec![3, 2, 1]),
                    blob_tx_hash: blob_transaction.hashed(),
                    hyle_output: HyleOutput {
                        version: 1,
                        initial_state,
                        next_state,
                        identity,
                        tx_hash: blob_transaction.hashed(),
                        tx_ctx: None,
                        index: blob_index,
                        tx_blob_count: blob_tx.blobs.len(),
                        blobs: blob_tx.blobs.clone().into(),
                        success: true,
                        state_reads: vec![],
                        onchain_effects: vec![],
                        program_outputs: vec![],
                    },
                }],
                is_recursive: false,
                proof: Some(proof),
            }),
        }
    }

    async fn assert_tx_status(
        server: &TestServer,
        tx_hash: TxHash,
        tx_status: TransactionStatusDb,
    ) {
        let transactions_response = server
            .get(format!("/transaction/hash/{tx_hash}").as_str())
            .await;
        transactions_response.assert_status_ok();
        let json_response = transactions_response.json::<APITransaction>();
        assert_eq!(
            json_response.transaction_status, tx_status,
            "Transaction status mismatch for tx_hash: {tx_hash}"
        );
    }

    async fn assert_tx_not_found(server: &TestServer, tx_hash: TxHash) {
        let transactions_response = server
            .get(format!("/transaction/hash/{tx_hash}").as_str())
            .await;
        transactions_response.assert_status_not_found();
    }

    #[test_log::test(tokio::test)]
    async fn test_indexer_handle_block_flow() -> Result<()> {
        let container = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .unwrap();
        let db = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgresql://postgres:postgres@localhost:{}/postgres",
                container.get_host_port_ipv4(5432).await.unwrap()
            ))
            .await
            .unwrap();
        MIGRATOR.run(&db).await.unwrap();

        let (mut indexer, explorer) = new_indexer(db).await;
        let server = setup_test_server(&explorer).await?;

        let initial_state = StateCommitment(vec![1, 2, 3]);
        let next_state = StateCommitment(vec![4, 5, 6]);
        let first_contract_name = ContractName::new("c1");
        let second_contract_name = ContractName::new("c2");

        let register_tx_1 = new_register_tx(first_contract_name.clone(), initial_state.clone());
        let register_tx_2 = new_register_tx(second_contract_name.clone(), initial_state.clone());

        let blob_transaction = new_blob_tx(
            Identity::new("test@c1"),
            first_contract_name.clone(),
            second_contract_name.clone(),
        );
        let blob_transaction_hash = blob_transaction.hashed();

        let proof_tx_1 = new_proof_tx(
            Identity::new("test@c1"),
            first_contract_name.clone(),
            BlobIndex(0),
            &blob_transaction,
            initial_state.clone(),
            next_state.clone(),
        );

        let proof_tx_2 = new_proof_tx(
            Identity::new("test@c1"),
            second_contract_name.clone(),
            BlobIndex(1),
            &blob_transaction,
            initial_state.clone(),
            next_state.clone(),
        );

        let other_blob_transaction = new_blob_tx(
            Identity::new("test@c1"),
            second_contract_name.clone(),
            first_contract_name.clone(),
        );
        let other_blob_transaction_hash = other_blob_transaction.hashed();
        // Send two proofs for the same blob
        let proof_tx_3 = new_proof_tx(
            Identity::new("test@c1"),
            first_contract_name.clone(),
            BlobIndex(1),
            &other_blob_transaction,
            StateCommitment(vec![7, 7, 7]),
            StateCommitment(vec![9, 9, 9]),
        );
        let proof_tx_4 = new_proof_tx(
            Identity::new("test@c1"),
            first_contract_name.clone(),
            BlobIndex(1),
            &other_blob_transaction,
            StateCommitment(vec![8, 8]),
            StateCommitment(vec![9, 9]),
        );

        let txs = vec![
            register_tx_1.into(),
            register_tx_2.into(),
            blob_transaction,
            proof_tx_1,
            proof_tx_2,
            other_blob_transaction,
            proof_tx_3,
            proof_tx_4,
        ];

        let mut node_state = NodeState {
            store: NodeStateStore::default(),
            metrics: NodeStateMetrics::global("test".to_string(), "test"),
        };

        // Handling a block containing txs

        let parent_data_proposal = DataProposal::new(None, txs);
        let mut signed_block = SignedBlock::default();
        signed_block.consensus_proposal.slot = 1;
        signed_block.data_proposals.push((
            LaneId(ValidatorPublicKey("ttt".into())),
            vec![parent_data_proposal.clone()],
        ));
        let block = node_state.force_handle_block(&signed_block);

        indexer
            .handle_processed_block(block)
            .expect("Failed to handle block");
        indexer
            .dump_store_to_db()
            .await
            .expect("Failed to dump store to DB");

        //
        // Handling MempoolStatusEvent
        //

        let initial_state_wd = StateCommitment(vec![1, 2, 3]);
        let next_state_wd = StateCommitment(vec![4, 5, 6]);
        let first_contract_name_wd = ContractName::new("wd1");
        let second_contract_name_wd = ContractName::new("wd2");

        let register_tx_1_wd =
            new_register_tx(first_contract_name_wd.clone(), initial_state_wd.clone());
        let register_tx_2_wd =
            new_register_tx(second_contract_name_wd.clone(), initial_state_wd.clone());

        let blob_transaction_wd = new_blob_tx(
            Identity::new("test@wd1"),
            first_contract_name_wd.clone(),
            second_contract_name_wd.clone(),
        );

        let proof_tx_1_wd = new_proof_tx(
            Identity::new("test@wd1"),
            first_contract_name_wd.clone(),
            BlobIndex(0),
            &blob_transaction_wd,
            initial_state_wd.clone(),
            next_state_wd.clone(),
        );

        let register_tx_1_wd = Transaction {
            version: 1,
            transaction_data: TransactionData::Blob(register_tx_1_wd),
        };
        let register_tx_2_wd = Transaction {
            version: 1,
            transaction_data: TransactionData::Blob(register_tx_2_wd),
        };

        indexer
            .handle_mempool_status_event(MempoolStatusEvent::WaitingDissemination {
                parent_data_proposal_hash: parent_data_proposal.hashed(),
                tx: register_tx_1_wd.clone(),
            })
            .await
            .expect("MempoolStatusEvent");

        assert_tx_status(
            &server,
            register_tx_1_wd.hashed(),
            TransactionStatusDb::WaitingDissemination,
        )
        .await;

        indexer
            .handle_mempool_status_event(MempoolStatusEvent::WaitingDissemination {
                parent_data_proposal_hash: parent_data_proposal.hashed(),
                tx: register_tx_2_wd.clone(),
            })
            .await
            .expect("MempoolStatusEvent");

        assert_tx_status(
            &server,
            register_tx_2_wd.hashed(),
            TransactionStatusDb::WaitingDissemination,
        )
        .await;

        indexer
            .handle_mempool_status_event(MempoolStatusEvent::WaitingDissemination {
                parent_data_proposal_hash: parent_data_proposal.hashed(),
                tx: blob_transaction_wd.clone(),
            })
            .await
            .expect("MempoolStatusEvent");

        assert_tx_status(
            &server,
            blob_transaction_wd.hashed(),
            TransactionStatusDb::WaitingDissemination,
        )
        .await;

        assert_tx_not_found(&server, proof_tx_1_wd.hashed()).await;

        let parent_data_proposal_hash = parent_data_proposal.hashed();

        // We skip blob_transaction_wd
        let data_proposal = DataProposal::new(
            Some(parent_data_proposal_hash.clone()),
            vec![register_tx_1_wd.clone(), register_tx_2_wd.clone()],
        );

        let data_proposal_created_event = MempoolStatusEvent::DataProposalCreated {
            parent_data_proposal_hash: parent_data_proposal_hash.clone(),
            data_proposal_hash: data_proposal.hashed(),
            txs_metadatas: vec![
                register_tx_1_wd.metadata(parent_data_proposal_hash.clone()),
                register_tx_2_wd.metadata(parent_data_proposal_hash.clone()),
            ],
        };

        indexer
            .handle_mempool_status_event(data_proposal_created_event.clone())
            .await
            .expect("MempoolStatusEvent");

        assert_tx_status(
            &server,
            register_tx_1_wd.hashed(),
            TransactionStatusDb::DataProposalCreated,
        )
        .await;
        assert_tx_status(
            &server,
            register_tx_2_wd.hashed(),
            TransactionStatusDb::DataProposalCreated,
        )
        .await;
        assert_tx_status(
            &server,
            blob_transaction_wd.hashed(),
            TransactionStatusDb::WaitingDissemination,
        )
        .await;
        assert_tx_not_found(&server, proof_tx_1_wd.hashed()).await;

        // We skip blob_transaction_wd
        let data_proposal_2 = DataProposal::new(
            Some(data_proposal.hashed()),
            vec![
                blob_transaction_wd.clone(),
                blob_transaction_wd.clone(),
                proof_tx_1_wd.clone(),
            ],
        );

        indexer
            .handle_mempool_status_event(MempoolStatusEvent::DataProposalCreated {
                parent_data_proposal_hash: data_proposal.hashed(),
                data_proposal_hash: data_proposal_2.hashed(),
                txs_metadatas: vec![
                    blob_transaction_wd.metadata(data_proposal.hashed()),
                    blob_transaction_wd.metadata(data_proposal.hashed()),
                    proof_tx_1_wd.metadata(data_proposal.hashed()),
                ],
            })
            .await
            .expect("MempoolStatusEvent");

        assert_tx_status(
            &server,
            blob_transaction_wd.hashed(),
            TransactionStatusDb::DataProposalCreated,
        )
        .await;

        let mut signed_block = SignedBlock::default();
        signed_block.consensus_proposal.timestamp = TimestampMs(12345);
        signed_block.consensus_proposal.slot = 2;
        signed_block.data_proposals.push((
            LaneId(ValidatorPublicKey("ttt".into())),
            vec![data_proposal, data_proposal_2],
        ));
        let block_2 = node_state.force_handle_block(&signed_block);
        let block_2_hash = block_2.hash.clone();
        indexer
            .handle_processed_block(block_2)
            .expect("Failed to handle block");
        indexer
            .dump_store_to_db()
            .await
            .expect("Failed to dump store to DB");

        assert_tx_status(
            &server,
            register_tx_1_wd.hashed(),
            TransactionStatusDb::Success,
        )
        .await;
        assert_tx_status(
            &server,
            register_tx_2_wd.hashed(),
            TransactionStatusDb::Success,
        )
        .await;
        assert_tx_status(
            &server,
            blob_transaction_wd.hashed(),
            TransactionStatusDb::Sequenced,
        )
        .await;
        assert_tx_not_found(&server, proof_tx_1_wd.hashed()).await;

        // Check a mempool status event does not change a Success/Sequenced status
        indexer
            .handle_mempool_status_event(data_proposal_created_event.clone())
            .await
            .expect("MempoolStatusEvent");

        // Check blocks have correct data
        let blocks = server.get("/blocks").await.json::<Vec<APIBlock>>();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks.last().unwrap().timestamp, 0);
        assert_eq!(blocks.first().unwrap().timestamp, 12345);

        let transactions_response = server.get("/contract/c1").await;
        transactions_response.assert_status_ok();
        let json_response = transactions_response.json::<APIContract>();
        assert_eq!(json_response.state_commitment, next_state.0);

        let transactions_response = server.get("/contract/c2").await;
        transactions_response.assert_status_ok();
        let json_response = transactions_response.json::<APIContract>();
        assert_eq!(json_response.state_commitment, next_state.0);

        let transactions_response = server.get("/contract/d1").await;
        transactions_response.assert_status_not_found();

        let blob_transactions_response = server.get("/blob_transactions/contract/c1").await;
        blob_transactions_response.assert_status_ok();
        assert_json_include!(
            actual: blob_transactions_response.json::<serde_json::Value>(),
            expected: json!([
                {
                    "blobs": [{
                        "contract_name": "c1",
                        "data": hex::encode([1,2,3]),
                        "proof_outputs": [
                            {
                                "initial_state": [7,7,7],
                            },
                            {
                                "initial_state": [8,8],
                            }
                        ]
                    }],
                    "transaction_status": "Sequenced",
                    "tx_hash": other_blob_transaction_hash.to_string(),
                    "index": 5,
                },
                {
                    "blobs": [{
                        "contract_name": "c1",
                        "data": hex::encode([1,2,3]),
                        "proof_outputs": [{}]
                    }],
                    "tx_hash": blob_transaction_hash.to_string(),
                    "index": 2,
                }
            ])
        );
        let all_txs = server.get("/transactions/block/1").await;
        all_txs.assert_status_ok();
        assert_json_include!(
            actual: all_txs.json::<serde_json::Value>(),
            expected: json!([
                { "index": 5, "transaction_type": "BlobTransaction", "transaction_status": "Sequenced" },
                { "index": 2, "transaction_type": "BlobTransaction", "transaction_status": "Success" },
                { "index": 1, "transaction_type": "BlobTransaction", "transaction_status": "Success" },
                { "index": 0, "transaction_type": "BlobTransaction", "transaction_status": "Success" },
            ])
        );

        let blob_transactions_response = server.get("/blob_transactions/contract/c2").await;
        blob_transactions_response.assert_status_ok();
        assert_json_include!(
            actual: blob_transactions_response.json::<serde_json::Value>(),
            expected: json!([
                {
                    "blobs": [{
                        "contract_name": "c2",
                        "data": hex::encode([1,2,3]),
                        "proof_outputs": []
                    }],
                    "tx_hash": other_blob_transaction_hash.to_string(),
                },
                {
                    "blobs": [{
                        "contract_name": "c2",
                        "data": hex::encode([1,2,3]),
                        "proof_outputs": [{}]
                    }],
                    "tx_hash": blob_transaction_hash.to_string(),
                }
            ])
        );

        // Test proof transaction endpoints
        let proofs_response = server.get("/proofs").await;
        proofs_response.assert_status_ok();
        assert_json_include!(
            actual: proofs_response.json::<serde_json::Value>(),
            expected: json!([
                { "index": 4, "transaction_type": "ProofTransaction", "transaction_status": "Success", "block_hash": block_2_hash },
                { "index": 7, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 6, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 4, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 3, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            ])
        );

        let proofs_by_height = server.get("/proofs/block/1").await;
        proofs_by_height.assert_status_ok();

        assert_json_include!(
            actual: proofs_by_height.json::<serde_json::Value>(),
            expected: json!([
                { "index": 7, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 6, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 4, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 3, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            ])
        );

        let proof_by_hash = server
            .get(format!("/proof/hash/{}", proof_tx_1_wd.hashed()).as_str())
            .await;
        proof_by_hash.assert_status_ok();
        assert_json_include!(
            actual: proof_by_hash.json::<serde_json::Value>(),
            expected: json!({
                "index": 4,
                "transaction_type": "ProofTransaction",
                "transaction_status": "Success"
            })
        );

        // Test non-existent proof
        let non_existent_proof = server
            .get("/proof/hash/1111111111111111111111111111111111111111111111111111111111111111")
            .await;
        non_existent_proof.assert_status_not_found();

        Ok(())
    }

    pub fn make_register_hyli_wallet_identity_tx() -> BlobTransaction {
        let mut tx = ProvableBlobTx::new("hyli@wallet".into());
        register_identity(&mut tx, "wallet".into(), "password".into()).unwrap();
        BlobTransaction::new("hyli@wallet".to_string(), tx.blobs)
    }

    async fn scenario_contracts() -> Result<(ContainerAsync<Postgres>, Indexer, Block, Block, Block)>
    {
        let container = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .unwrap();
        let db = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgresql://postgres:postgres@localhost:{}/postgres",
                container.get_host_port_ipv4(5432).await.unwrap()
            ))
            .await
            .unwrap();
        MIGRATOR.run(&db).await.unwrap();
        let (indexer, _) = new_indexer(db).await;

        let mut node_state = NodeState {
            store: NodeStateStore::default(),
            metrics: NodeStateMetrics::global("test".to_string(), "test"),
        };

        let register_wallet = new_register_tx("wallet".into(), StateCommitment(vec![]));
        let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

        let register_hyli_at_wallet_proof = new_proof_tx(
            "hyli@wallet".into(),
            "wallet".into(),
            BlobIndex(0),
            &register_hyli_at_wallet.clone().into(),
            StateCommitment(vec![]),
            StateCommitment(vec![0]),
        );

        node_state.craft_block_and_handle(
            1,
            vec![
                register_wallet.into(),
                register_hyli_at_wallet.into(),
                register_hyli_at_wallet_proof,
            ],
        );

        // Create a couple fake blocks with contracts
        let b1 = node_state.craft_block_and_handle(
            3,
            vec![
                new_register_tx(ContractName::new("a"), StateCommitment(vec![])).into(),
                new_register_tx(ContractName::new("b"), StateCommitment(vec![])).into(),
                new_register_tx(ContractName::new("c"), StateCommitment(vec![])).into(),
            ],
        );

        let delete_a = new_delete_tx(ContractName::new("hyle"), ContractName::new("a"));
        let delete_c = new_delete_tx(ContractName::new("hyle"), ContractName::new("c"));

        let delete_a_proof = new_proof_tx(
            "hyli@wallet".into(),
            "wallet".into(),
            BlobIndex(0),
            &delete_a.clone().into(),
            StateCommitment(vec![0]),
            StateCommitment(vec![1]),
        );
        let delete_c_proof = new_proof_tx(
            "hyli@wallet".into(),
            "wallet".into(),
            BlobIndex(0),
            &delete_c.clone().into(),
            StateCommitment(vec![1]),
            StateCommitment(vec![2]),
        );

        let b2 = node_state.craft_block_and_handle(
            4,
            vec![
                delete_a.into(),
                delete_a_proof,
                delete_c.into(),
                delete_c_proof,
            ],
        );

        let delete_b = new_delete_tx(ContractName::new("hyle"), ContractName::new("b"));
        let delete_b_proof = new_proof_tx(
            "hyli@wallet".into(),
            "wallet".into(),
            BlobIndex(0),
            &delete_b.clone().into(),
            StateCommitment(vec![2]),
            StateCommitment(vec![3]),
        );

        let delete_a = new_delete_tx(ContractName::new("hyle"), ContractName::new("a"));
        let delete_a_proof = new_proof_tx(
            "hyli@wallet".into(),
            "wallet".into(),
            BlobIndex(0),
            &delete_a.clone().into(),
            StateCommitment(vec![3]),
            StateCommitment(vec![4]),
        );

        let delete_d = new_delete_tx(ContractName::new("hyle"), ContractName::new("d"));
        let delete_d_proof = new_proof_tx(
            "hyli@wallet".into(),
            "wallet".into(),
            BlobIndex(0),
            &delete_d.clone().into(),
            StateCommitment(vec![4]),
            StateCommitment(vec![5]),
        );

        let b3 = node_state.craft_block_and_handle_with_parent_dp_hash(
            5,
            vec![
                new_register_tx(ContractName::new("a"), StateCommitment(vec![])).into(),
                delete_b.into(),
                delete_b_proof,
                delete_a.into(),
                delete_a_proof,
                new_register_tx(ContractName::new("a"), StateCommitment(vec![])).into(),
                new_register_tx(ContractName::new("d"), StateCommitment(vec![])).into(),
                delete_d.into(),
                delete_d_proof,
            ],
            DataProposalHash("test".to_string()),
        );

        Ok((container, indexer, b1, b2, b3))
    }

    #[test_log::test(tokio::test)]
    async fn test_contracts_dump_every_block() -> Result<()> {
        let (_c, mut indexer, b1, b2, b3) = scenario_contracts().await?;

        indexer.handle_processed_block(b1.clone()).unwrap();
        indexer.dump_store_to_db().await.unwrap();
        let rows = sqlx::query("SELECT * FROM contracts")
            .fetch_all(&indexer.db)
            .await
            .context("fetch contracts")?;
        assert_eq!(
            rows.iter()
                .map(|r| r.get::<String, _>("contract_name"))
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );

        indexer.handle_processed_block(b2.clone()).unwrap();
        indexer.dump_store_to_db().await.unwrap();
        let rows = sqlx::query("SELECT * FROM contracts")
            .fetch_all(&indexer.db)
            .await
            .context("fetch contracts")?;
        assert_eq!(
            rows.iter()
                .map(|r| r.get::<String, _>("contract_name"))
                .collect::<Vec<_>>(),
            vec!["b"]
        );

        indexer.handle_processed_block(b3.clone()).unwrap();
        indexer.dump_store_to_db().await.unwrap();

        let rows = sqlx::query("SELECT * FROM contracts")
            .fetch_all(&indexer.db)
            .await
            .context("fetch contracts")?;
        assert_eq!(
            rows.iter()
                .map(|r| r.get::<String, _>("contract_name"))
                .collect::<Vec<_>>(),
            vec!["a"]
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_contracts_batched() -> Result<()> {
        let (_c, mut indexer, b1, b2, b3) = scenario_contracts().await?;
        indexer.handle_processed_block(b1).unwrap();
        indexer.handle_processed_block(b2).unwrap();
        indexer.handle_processed_block(b3).unwrap();
        indexer.dump_store_to_db().await.unwrap();

        let rows = sqlx::query("SELECT * FROM contracts")
            .fetch_all(&indexer.db)
            .await
            .context("fetch contracts")?;
        assert_eq!(
            rows.iter()
                .map(|r| r.get::<String, _>("contract_name"))
                .collect::<Vec<_>>(),
            vec!["a"]
        );
        Ok(())
    }

    // In case of duplicate tx hash, should return information of the tx with the highest block height
    // or index (position in the block)
    #[test_log::test(tokio::test)]
    async fn test_indexer_api_doubles() -> Result<()> {
        let container = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .unwrap();
        let db = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgresql://postgres:postgres@localhost:{}/postgres",
                container.get_host_port_ipv4(5432).await.unwrap()
            ))
            .await
            .unwrap();
        MIGRATOR.run(&db).await.unwrap();
        sqlx::raw_sql(include_str!("../tests/fixtures/test_data.sql"))
            .execute(&db)
            .await
            .context("insert test data")?;

        let (_indexer, explorer) = new_indexer(db).await;
        let server = setup_test_server(&explorer).await?;

        // Multiple txs with same hash -- all in different blocks

        let transactions_response = server
            .get("/transaction/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<APITransaction>();
        assert_eq!(
            result.parent_dp_hash.0,
            "dp_hashbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()
        );

        // Multiple txs with same hash -- one not yet in a block, should return the pending one

        let transactions_response = server
            .get("/proof/hash/test_tx_hash_3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<APITransaction>();
        assert_eq!(
            result.parent_dp_hash.0,
            "dp_hashbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()
        );
        assert_eq!(result.block_hash, None);

        // Get blobs by tx hash

        let transactions_response = server
            .get("/blobs/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<Vec<APIBlob>>();
        assert!(result.len() == 1);
        assert_eq!(
            result.first().unwrap().data,
            "{\"data\": \"blob_data_2_bis\"}".as_bytes()
        );

        // Get blob by tx hash

        let transactions_response = server
            .get("/blob/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index/0")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<APIBlob>();
        assert_eq!(result.data, "{\"data\": \"blob_data_2_bis\"}".as_bytes());

        // Get proof by tx hash

        let transactions_response = server
            .get("/proof/hash/test_tx_hash_3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<APITransaction>();
        assert_eq!(
            result.parent_dp_hash.0,
            "dp_hashbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()
        );
        assert_eq!(result.block_hash, None);

        // Get transaction state event, the latest one

        let transactions_response = server
            .get("/transaction/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/events")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<Vec<APITransactionEvents>>();
        assert_eq!(
            result,
            vec![APITransactionEvents {
                block_hash: ConsensusProposalHash(
                    "block3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()
                ),
                block_height: BlockHeight(3),
                events: vec![serde_json::json!({
                    "name": "Success"
                })]
            }]
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_indexer_api() -> Result<()> {
        let container = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .unwrap();
        let db = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgresql://postgres:postgres@localhost:{}/postgres",
                container.get_host_port_ipv4(5432).await.unwrap()
            ))
            .await
            .unwrap();
        MIGRATOR.run(&db).await.unwrap();
        sqlx::raw_sql(include_str!("../tests/fixtures/test_data.sql"))
            .execute(&db)
            .await
            .context("insert test data")?;

        let (_indexer, mut explorer) = new_indexer(db).await;
        let server = setup_test_server(&explorer).await?;

        // Blocks
        // Get all blocks
        let transactions_response = server.get("/blocks").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Test pagination
        let transactions_response = server.get("/blocks?nb_results=1").await;
        transactions_response.assert_status_ok();
        assert_eq!(transactions_response.json::<Vec<APIBlock>>().len(), 1);
        assert_eq!(
            transactions_response
                .json::<Vec<APIBlock>>()
                .first()
                .unwrap()
                .height,
            3
        );
        let transactions_response = server.get("/blocks?nb_results=1&start_block=1").await;
        transactions_response.assert_status_ok();
        assert_eq!(transactions_response.json::<Vec<APIBlock>>().len(), 1);
        assert_eq!(
            transactions_response
                .json::<Vec<APIBlock>>()
                .first()
                .unwrap()
                .height,
            1
        );
        // Test negative end of blocks
        let transactions_response = server.get("/blocks?nb_results=10&start_block=4").await;
        transactions_response.assert_status_ok();

        // Get the last block
        let transactions_response = server.get("/block/last").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get block by height
        let transactions_response = server.get("/block/height/1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get block by hash
        let transactions_response = server
            .get("/block/hash/block1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Transactions
        // Get all transactions
        let transactions_response = server.get("/transactions").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get all transactions by height
        let transactions_response = server.get("/transactions/block/2").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get an existing transaction by name
        let transactions_response = server.get("/transactions/contract/contract_1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get an unknown transaction by name
        let transactions_response = server.get("/transactions/contract/unknown_contract").await;
        transactions_response.assert_status_ok();
        assert_eq!(transactions_response.text(), "[]");

        // Get an existing transaction by hash
        let transactions_response = server
            .get("/transaction/hash/test_tx_hash_1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get an existing transaction, waiting for dissemination by hash
        let transactions_response = server
            .get("/transaction/hash/test_tx_hash_0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get an unknown transaction by hash
        let unknown_tx = server.get("/transaction/hash/1111111111111111111111111111111111111111111111111111111111111111").await;
        unknown_tx.assert_status_not_found();

        // Blobs
        // Get all transactions for a specific contract name
        let transactions_response = server.get("/blob_transactions/contract/contract_1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get blobs by tx_hash
        let transactions_response = server
            .get("/blobs/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get unknown blobs by tx_hash
        let transactions_response = server
            .get("/blobs/hash/test_tx_hash_1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert_eq!(transactions_response.text(), "[]");

        // Get blob by tx_hash and index
        let transactions_response = server
            .get("/blob/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index/0")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get blob by tx_hash and unknown index
        let transactions_response = server
            .get("/blob/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index/1000")
            .await;
        transactions_response.assert_status_not_found();

        // Contracts
        // Get contract by name
        let transactions_response = server.get("/contract/contract_1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get contract state by name and height
        let transactions_response = server.get("/state/contract/contract_1/block/1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Websocket
        let listener = hyle_net::net::bind_tcp_listener(0).await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(axum::serve(listener, explorer.api(None)).into_future());

        let _ = tokio_tungstenite::connect_async(format!(
            "ws://{addr}/blob_transactions/contract/contract_1/ws"
        ))
        .await
        .unwrap();

        if let Some(tx) = explorer.new_sub_receiver.recv().await {
            let (contract_name, _) = tx;
            assert_eq!(contract_name, ContractName::new("contract_1"));
        }

        Ok(())
    }
}
