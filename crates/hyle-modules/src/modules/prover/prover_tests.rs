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

impl sdk::FullStateRevert for TestContract {}

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
    api_client.add_contract(Contract {
        name: "test".into(),
        state: TestContract::default().commit(),
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        timeout_window: TimeoutWindow::Timeout(BlockHeight(timeout)),
    });

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
        api: None,
        node: api_client,
        default_state: TestContract::default(),
        buffer_blocks,
        max_txs_per_proof,
        tx_working_window_size: max_txs_per_proof,
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

    auto_prover.handle_block(block_1).await?;

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

    tracing::info!("✨ Block 2");
    let block_2 = node_state.craft_block_and_handle(2, proofs);
    auto_prover.handle_block(block_2).await?;

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
    auto_prover.handle_block(block_3).await?;

    // Proofs 3 won't be sent, to trigger a timeout
    let proofs_3 = get_txs(&api_client).await;
    assert_eq!(proofs_3.len(), 1);

    tracing::info!("✨ Block 4");
    let block_4 =
        node_state.craft_block_and_handle(4, vec![new_blob_tx(4), new_blob_tx(4), new_blob_tx(4)]);
    auto_prover.handle_block(block_4).await?;

    // No proof at this point.
    let proofs_4 = get_txs(&api_client).await;
    assert_eq!(proofs_4.len(), 0);

    for i in 5..15 {
        tracing::info!("✨ Block {i}");
        let block = node_state.craft_block_and_handle(i, vec![]);
        auto_prover.handle_block(block).await?;
    }

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 2);

    let _block_11 = node_state.craft_block_and_handle(16, proofs);
    assert_eq!(read_contract_state(&node_state).value, 4);

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
    let block_2 = node_state.craft_block_and_handle(2, proofs);
    assert_eq!(read_contract_state(&node_state).value, 0);
    auto_prover.handle_processed_block(block_2).await?;

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
    let block_2 = node_state.craft_block_and_handle(2, proofs);
    assert_eq!(read_contract_state(&node_state).value, 1);
    auto_prover.handle_processed_block(block_2).await?;

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
    let (mut node_state, _, api_client) = setup_with_timeout(10).await?;
    let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 5, 5).await?;

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

    // Process some blocks to pop buffer
    for i in 8..=12 {
        tracing::info!("✨ Block {i}");
        let block = node_state.craft_block_and_handle(i, vec![]);
        auto_prover.handle_processed_block(block).await?;
    }

    let proofs = get_txs(&api_client).await;

    tracing::info!("✨ Block 13");
    let block_13 = node_state.craft_block_and_handle(13, proofs);
    auto_prover.handle_processed_block(block_13).await?;
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
    let block_2 = node_state.craft_block_and_handle(2, proofs);
    assert_eq!(read_contract_state(&node_state).value, 0);

    auto_prover.handle_processed_block(block_2).await?;

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
async fn test_auto_prover_duplicated_tx() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;

    let tx = new_blob_tx(1);

    tracing::info!("✨ Block 1");
    let block_1 = node_state.craft_block_and_handle(1, vec![tx.clone()]);
    auto_prover.handle_processed_block(block_1.clone()).await?;
    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

    tracing::info!("✨ Block 2");
    let block_2 = node_state.craft_block_and_handle(2, vec![tx.clone()]);
    auto_prover.handle_block(block_2.clone()).await?;
    let proofs_2 = get_txs(&api_client).await;
    assert_eq!(proofs_2.len(), 0);

    tracing::info!("✨ Block 3");
    let block_3 = node_state.craft_block_and_handle(3, proofs);
    auto_prover.handle_block(block_3.clone()).await?;
    let proofs_3 = get_txs(&api_client).await;
    assert_eq!(proofs_3.len(), 0);

    tracing::info!("✨ Block 4");
    let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(2)]);
    auto_prover.handle_block(block_4.clone()).await?;
    let proofs_4 = get_txs(&api_client).await;
    assert_eq!(proofs_4.len(), 1);

    tracing::info!("✨ Block 5");
    let block_5 = node_state.craft_block_and_handle(5, proofs_4);
    assert_eq!(read_contract_state(&node_state).value, 1 + 2);

    tracing::info!("✨ New prover catching up");
    api_client.set_block_height(BlockHeight(5));

    let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
        .await
        .expect("Failed to create new auto prover");

    let blocks = [block_1, block_2, block_3, block_4, block_5];
    for block in blocks {
        auto_prover_catchup.handle_block(block).await?;
    }

    tracing::info!("✨ Block 6");
    let block_6 = node_state.craft_block_and_handle(6, vec![new_blob_tx(6)]);
    auto_prover_catchup.handle_block(block_6).await?;
    let proofs_6 = get_txs(&api_client).await;
    assert_eq!(proofs_6.len(), 1);
    tracing::info!("✨ Block 7");
    let _block_7 = node_state.craft_block_and_handle(7, proofs_6);
    assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 6);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_auto_prover_two_blobs_in_tx() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;

    let tx = BlobTransaction::new(
        "yolo@test".to_string(),
        vec![
            Blob {
                contract_name: "test".into(),
                data: BlobData(borsh::to_vec(&2).unwrap()),
            },
            Blob {
                contract_name: "test".into(),
                data: BlobData(borsh::to_vec(&3).unwrap()),
            },
        ],
    );

    tracing::info!("✨ Block 1");
    let block_1 = node_state.craft_block_and_handle(1, vec![tx.into(), new_blob_tx(4)]);
    auto_prover.handle_processed_block(block_1).await?;
    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);
    tracing::info!("✨ Block 2");
    let block_2 = node_state.craft_block_and_handle(2, proofs);
    auto_prover.handle_processed_block(block_2).await?;
    assert_eq!(read_contract_state(&node_state).value, 2 + 3 + 4);

    let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(5)]);
    auto_prover.handle_processed_block(block_3).await?;
    let proofs_3 = get_txs(&api_client).await;
    assert_eq!(proofs_3.len(), 1);
    tracing::info!("✨ Block 4");
    let _ = node_state.craft_block_and_handle(4, proofs_3);
    assert_eq!(read_contract_state(&node_state).value, 2 + 3 + 4 + 5);

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
    let (mut node_state, _, api_client) = setup().await?;
    let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 3, 10).await?;

    let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
    let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
    let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

    auto_prover.handle_processed_block(block_1.clone()).await?;
    auto_prover.handle_processed_block(block_2.clone()).await?;
    auto_prover.handle_processed_block(block_3.clone()).await?;

    let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
    auto_prover.handle_processed_block(block_4.clone()).await?;

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

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
    let (mut node_state, _, api_client) = setup().await?;
    let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 3, 10).await?;

    let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
    let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
    let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

    auto_prover.handle_processed_block(block_1.clone()).await?;
    auto_prover.handle_processed_block(block_2.clone()).await?;
    auto_prover.handle_processed_block(block_3.clone()).await?;

    let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
    auto_prover.handle_processed_block(block_4.clone()).await?;

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

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
    let (mut node_state, _, api_client) = setup().await?;
    let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 3, 10).await?;

    let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
    let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
    let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

    auto_prover.handle_processed_block(block_1.clone()).await?;
    auto_prover.handle_processed_block(block_2.clone()).await?;
    auto_prover.handle_processed_block(block_3.clone()).await?;

    let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
    auto_prover.handle_processed_block(block_4.clone()).await?;

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

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
    let (mut node_state, _, api_client) = setup().await?;
    let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 3, 10).await?;

    let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
    let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
    let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

    auto_prover.handle_processed_block(block_1.clone()).await?;
    auto_prover.handle_processed_block(block_2.clone()).await?;
    auto_prover.handle_processed_block(block_3.clone()).await?;

    let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
    auto_prover.handle_processed_block(block_4.clone()).await?;

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

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
        block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8, block_9, block_10,
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
    let (mut node_state, _, api_client) = setup().await?;
    let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 3, 10).await?;

    let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
    let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
    let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

    auto_prover.handle_processed_block(block_1.clone()).await?;
    auto_prover.handle_processed_block(block_2.clone()).await?;
    auto_prover.handle_processed_block(block_3.clone()).await?;

    let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
    auto_prover.handle_processed_block(block_4.clone()).await?;

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

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
        block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8, block_9, block_10,
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

    // First proof of 2 txs
    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

    tracing::info!("✨ Block 5");
    let block_5 = node_state.craft_block_and_handle(5, proofs);
    auto_prover.handle_processed_block(block_5).await?;

    // Proof of 2 next txs
    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

    tracing::info!("✨ Block 6");
    let block_6 = node_state.craft_block_and_handle(6, proofs);
    auto_prover.handle_processed_block(block_6).await?;

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
    assert_eq!(proofs.len(), 1);

    tracing::info!("✨ Block 5");
    let block_5 = node_state.craft_block_and_handle(5, proofs);
    auto_prover.handle_processed_block(block_5).await?;

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

    tracing::info!("✨ Block 6");
    let block_6 = node_state.craft_block_and_handle(6, proofs);
    auto_prover.handle_processed_block(block_6).await?;

    assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3 + 4);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_auto_prover_artificial_middle_blob_failure_nobuffering() -> Result<()> {
    let (node_state, api_client) =
        scenario_auto_prover_artificial_middle_blob_failure_setup().await;

    let auto_prover = new_simple_auto_prover(api_client.clone()).await?;

    scenario_auto_prover_artificial_middle_blob_failure(node_state, api_client, auto_prover)
        .await
        .expect("Failed to run scenario");

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_auto_prover_artificial_middle_blob_failure_buffering() -> Result<()> {
    let (node_state, api_client) =
        scenario_auto_prover_artificial_middle_blob_failure_setup().await;

    let auto_prover = new_buffering_auto_prover(api_client.clone(), 10, 20).await?;

    scenario_auto_prover_artificial_middle_blob_failure(node_state, api_client, auto_prover)
        .await
        .expect("Failed to run scenario");

    Ok(())
}

async fn scenario_auto_prover_artificial_middle_blob_failure_setup(
) -> (NodeState, Arc<NodeApiMockClient>) {
    let mut node_state = new_node_state().await;
    let api_client = NodeApiMockClient::new();

    let register = RegisterContractEffect {
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        state_commitment: TestContract::default().commit(),
        contract_name: "test".into(),
        timeout_window: Some(TimeoutWindow::Timeout(BlockHeight(20))),
    };
    node_state.handle_register_contract_effect(&register);
    api_client.add_contract(Contract {
        name: ContractName::new("test"),
        state: TestContract::default().commit(),
        program_id: ProgramId(vec![]),
        verifier: "test".into(),
        timeout_window: TimeoutWindow::Timeout(BlockHeight(20)),
    });

    let register = RegisterContractEffect {
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        state_commitment: TestContract::default().commit(),
        contract_name: "test2".into(),
        timeout_window: Some(TimeoutWindow::Timeout(BlockHeight(20))),
    };
    node_state.handle_register_contract_effect(&register);
    api_client.add_contract(Contract {
        name: ContractName::new("test2"),
        state: TestContract::default().commit(),
        program_id: ProgramId(vec![]),
        verifier: "test".into(),
        timeout_window: TimeoutWindow::Timeout(BlockHeight(20)),
    });
    (node_state, Arc::new(api_client))
}

async fn scenario_auto_prover_artificial_middle_blob_failure(
    mut node_state: NodeState,
    api_client: Arc<NodeApiMockClient>,
    mut auto_prover: AutoProver<TestContract>,
) -> Result<()> {
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

    let mut ho = make_hyle_output_with_state(failing_tx_data.clone(), BlobIndex(0), &[4], &[34]);
    ho.success = false;
    let failing_proof = new_proof_tx(&ContractName::new("test2"), &ho, &failing_tx_data.hashed());

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
    block_4.dp_parent_hashes.insert(
        failing_tx_4.hashed(),
        DataProposalHash(format!("{:064x}", 4)),
    );
    block_4.dp_parent_hashes.insert(
        success_tx_2.hashed(),
        DataProposalHash(format!("{:064x}", 2)),
    );
    block_4.dp_parent_hashes.insert(
        failing_tx_1.hashed(),
        DataProposalHash(format!("{:064x}", 1)),
    );
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
            LaneId(ValidatorPublicKey(vec![0; 48])),
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

#[test_log::test(tokio::test)]
async fn test_auto_prover_serialize_and_resume() -> Result<()> {
    // Step 1: Run a couple of blocks with a node state
    let (mut node_state, mut prover, api_client) = setup_with_timeout(10).await?;
    let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);

    prover.handle_block(block_1.clone()).await?;

    let mut proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

    let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2), proofs.pop().unwrap()]);
    let block_3 = node_state.craft_block_and_handle(3, vec![]);
    let blocks = vec![block_1.clone(), block_2.clone(), block_3.clone()];

    api_client.set_block_height(BlockHeight(2));

    // Step 2: Start a prover, catch up with the blocks
    let temp_dir = tempdir()?;
    let data_dir = temp_dir.path().to_path_buf();
    let ctx = Arc::new(AutoProverCtx {
        data_directory: data_dir,
        prover: Arc::new(TxExecutorTestProver::<TestContract>::new()),
        contract_name: ContractName("test".into()),
        api: None,
        node: api_client.clone(),
        default_state: TestContract::default(),
        buffer_blocks: 0,
        max_txs_per_proof: 1,
        tx_working_window_size: 3,
    });

    let bus = SharedMessageBus::new(BusMetrics::global("default".to_string()));
    let mut auto_prover = AutoProver::<TestContract>::build(bus.new_handle(), ctx.clone())
        .await
        .unwrap();

    for block in &blocks {
        auto_prover.handle_block(block.clone()).await?;
    }

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);

    // Step 3: Serialize the prover state
    auto_prover.persist().await?;

    // Step 4: Run a couple more blocks
    let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]); //, proofs.pop().unwrap()]);
    let block_5 = node_state.craft_block_and_handle(5, vec![new_blob_tx(5)]);
    let block_6 = node_state.craft_block_and_handle(6, vec![]);
    let more_blocks = vec![block_4.clone(), block_5.clone(), block_6.clone()];

    api_client.set_block_height(BlockHeight(5));

    let mut auto_prover = AutoProver::<TestContract>::build(bus.new_handle(), ctx)
        .await
        .unwrap();

    // Step 6: Catch up again with all blocks
    for block in more_blocks.iter() {
        auto_prover.handle_block(block.clone()).await?;
    }

    auto_prover.handle_block(block_6.clone()).await?;

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 3);

    node_state.craft_block_and_handle(7, proofs);

    // Step 7: Check that the contract state is as expected
    let expected = 1 + 2 + 4 + 5;
    assert_eq!(read_contract_state(&node_state).value, expected);
    Ok(())
}
