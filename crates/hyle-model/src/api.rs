use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use strum::IntoDiscriminant;
use utoipa::ToSchema;

use crate::{
    utils::TimestampMs, BlockHash, BlockHeight, ConsensusProposalHash, ContractName,
    DataProposalHash, Identity, LaneBytesSize, LaneId, ProgramId, StateCommitment, TimeoutWindow,
    Transaction, TransactionKind, TxHash, ValidatorPublicKey, Verifier,
};

#[derive(Clone, Serialize, Deserialize, Debug, ToSchema)]
pub struct NodeInfo {
    pub id: String,
    pub pubkey: Option<ValidatorPublicKey>,
    pub da_address: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, ToSchema)]
pub struct NetworkStats {
    pub total_transactions: i64,           // Total number of transactions
    pub txs_last_day: i64,                 // Number of transactions in the last day
    pub total_contracts: i64,              // Total number of contracts
    pub contracts_last_day: i64,           // Number of contracts in the last day
    pub graph_tx_volume: Vec<(i64, i64)>,  // Graph data for transactions volume
    pub graph_block_time: Vec<(i64, f64)>, // Graph data for block time
    pub peak_txs: (i64, i64),              // Peak transactions per minutes in the last 24 hours
}

#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[derive(Clone, Serialize, Deserialize, Debug, ToSchema)]
pub struct ProofStat {
    pub verifier: String,
    pub proof_count: i64,
}

#[derive(Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct APIRegisterContract {
    pub verifier: Verifier,
    pub program_id: ProgramId,
    pub state_commitment: StateCommitment,
    pub contract_name: ContractName,
    pub timeout_window: Option<u64>,
    pub constructor_metadata: Option<Vec<u8>>,
}

/// Copy from Staking contract
#[derive(Debug, Default, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APIStaking {
    pub stakes: BTreeMap<Identity, u128>,
    pub delegations: BTreeMap<ValidatorPublicKey, Vec<Identity>>,
    /// When a validator distribute rewards, it is added in this list to
    /// avoid distributing twice the rewards for a same block
    pub rewarded: BTreeMap<ValidatorPublicKey, Vec<BlockHeight>>,

    /// List of validators that are part of consensus
    pub bonded: Vec<ValidatorPublicKey>,
    pub total_bond: u128,

    /// Struct to handle fees
    pub fees: APIFees,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APIFeesBalance {
    pub balance: i128,
    pub cumul_size: LaneBytesSize,
}

#[derive(Debug, Default, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APIFees {
    /// Balance of each validator
    pub balances: BTreeMap<ValidatorPublicKey, APIFeesBalance>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct APIBlock {
    // Struct for the blocks table
    pub hash: ConsensusProposalHash,
    pub parent_hash: ConsensusProposalHash,
    pub height: u64,    // Corresponds to BlockHeight
    pub timestamp: i64, // UNIX timestamp
    pub total_txs: u64, // Total number of transactions in the block
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct APIDataProposal {
    // Struct for the data_proposals table
    pub hash: DataProposalHash,                // The data proposal hash
    pub parent_hash: Option<DataProposalHash>, // Parent data proposal hash (nullable)
    pub lane_id: String,                       // Lane that created this proposal
    pub tx_count: u32,                         // Number of transactions
    pub estimated_size: u64,                   // Estimated size in bytes
    pub block_hash: ConsensusProposalHash,     // Block containing this DP
    pub block_height: u64,                     // Height of the block
    pub timestamp: i64, // Block timestamp as creation time (Unix timestamp in ms)
}

#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "transaction_type", rename_all = "snake_case")
)]
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub enum TransactionTypeDb {
    BlobTransaction,
    ProofTransaction,
    RegisterContractTransaction,
    Stake,
}

#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "transaction_status", rename_all = "snake_case")
)]
pub enum TransactionStatusDb {
    WaitingDissemination,
    DataProposalCreated,
    Success,
    Failure,
    Sequenced,
    TimedOut,
}

impl TransactionTypeDb {
    pub fn from(transaction: &Transaction) -> Self {
        transaction.transaction_data.discriminant().into()
    }
}

impl From<TransactionKind> for TransactionTypeDb {
    fn from(value: TransactionKind) -> Self {
        match value {
            TransactionKind::Blob => TransactionTypeDb::BlobTransaction,
            TransactionKind::Proof => TransactionTypeDb::ProofTransaction,
            TransactionKind::VerifiedProof => TransactionTypeDb::ProofTransaction,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APITransaction {
    // Struct for the transactions table
    pub tx_hash: TxHash,                           // Transaction hash
    pub parent_dp_hash: DataProposalHash,          // Data proposal hash
    pub block_hash: Option<ConsensusProposalHash>, // Corresponds to the block hash
    pub index: Option<u32>,                        // Index of the transaction within the block
    pub version: u32,                              // Transaction version
    pub transaction_type: TransactionTypeDb,       // Type of transaction
    pub transaction_status: TransactionStatusDb,   // Status of the transaction
    pub timestamp: Option<TimestampMs>,            // Timestamp of the transaction (block timestamp)
    pub lane_id: Option<LaneId>,                   // Lane ID where the transaction got disseminated
    pub identity: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APITransactionEvents {
    pub block_hash: BlockHash,
    pub block_height: BlockHeight,
    pub events: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq)]
pub struct TransactionWithBlobs {
    // Should match APITransaction
    pub tx_hash: TxHash,
    pub parent_dp_hash: DataProposalHash,
    pub block_hash: ConsensusProposalHash,
    pub index: u32,
    pub version: u32,
    pub transaction_type: TransactionTypeDb,
    pub transaction_status: TransactionStatusDb,
    pub timestamp: Option<TimestampMs>,
    pub lane_id: Option<LaneId>,
    pub identity: String,
    // Additional field for blobs
    pub blobs: Vec<BlobWithStatus>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APIProofDetails {
    // Should match APITransaction (but without identity)
    pub tx_hash: TxHash,                           // Transaction hash
    pub parent_dp_hash: DataProposalHash,          // Data proposal hash
    pub block_hash: Option<ConsensusProposalHash>, // Corresponds to the block hash
    pub index: Option<u32>,                        // Index of the transaction within the block
    pub version: u32,                              // Transaction version
    pub transaction_type: TransactionTypeDb,       // Type of transaction
    pub transaction_status: TransactionStatusDb,   // Status of the transaction
    pub timestamp: Option<TimestampMs>,            // Timestamp of the transaction (block timestamp)
    pub lane_id: Option<LaneId>,                   // Lane ID where the transaction got disseminated
    pub proof_outputs: Vec<(TxHash, u32, u32, serde_json::Value)>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BlobWithStatus {
    pub contract_name: String, // Contract name associated with the blob
    #[serde_as(as = "serde_with::hex::Hex")]
    pub data: Vec<u8>, // Actual blob data
    pub proof_outputs: Vec<serde_json::Value>, // outputs of proofs
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct APIContract {
    // Struct for the contracts table
    pub tx_hash: TxHash,  // Corresponds to the registration transaction hash
    pub verifier: String, // Verifier of the contract
    #[serde_as(as = "serde_with::hex::Hex")]
    pub program_id: Vec<u8>, // Program ID
    #[serde_as(as = "serde_with::hex::Hex")]
    pub state_commitment: Vec<u8>, // State commitment of the contract
    pub contract_name: String, // Contract name
    pub total_tx: u64,    // Total number of transactions associated with the contract
    pub unsettled_tx: u64, // Total number of unsettled transactions
    pub earliest_unsettled: Option<BlockHeight>, // Earliest unsettled transaction block height
}

#[derive(Debug, Serialize, ToSchema)]
pub struct APINodeContract {
    pub contract_name: ContractName,       // Name of the contract
    pub state_block_height: BlockHeight,   // Block height where the state is captured
    pub state_commitment: StateCommitment, // The contract state stored in JSON format
    pub program_id: ProgramId,             // Program ID of the contract
    pub verifier: Verifier,                // Verifier of the contract
    pub timeout_window: Option<u64>,       // Timeout window for the contract
}

impl<'de> Deserialize<'de> for APINodeContract {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper {
            Old(crate::Contract),
            New {
                contract_name: ContractName,
                state_block_height: BlockHeight,
                state_commitment: StateCommitment,
                program_id: ProgramId,
                verifier: Verifier,
                timeout_window: Option<u64>,
            },
        }

        match Helper::deserialize(deserializer)? {
            Helper::Old(c) => Ok(APINodeContract {
                contract_name: c.name,
                state_block_height: BlockHeight(0),
                state_commitment: c.state,
                program_id: c.program_id,
                verifier: c.verifier,
                timeout_window: match c.timeout_window {
                    TimeoutWindow::Timeout(timeout) => Some(timeout.0),
                    TimeoutWindow::NoTimeout => None,
                },
            }),
            Helper::New {
                contract_name,
                state_block_height,
                state_commitment,
                program_id,
                verifier,
                timeout_window,
            } => Ok(APINodeContract {
                contract_name,
                state_block_height,
                state_commitment,
                program_id,
                verifier,
                timeout_window,
            }),
        }
    }
}

#[test]
fn test_deserialize_api_node_contract() {
    // Test old format
    let old_json = serde_json::to_value(&crate::Contract {
        name: ContractName("my_contract".to_string()),
        program_id: ProgramId(vec![4, 5, 6]),
        state: StateCommitment(vec![1, 2, 3]),
        verifier: Verifier("verifier1".to_string()),
        timeout_window: TimeoutWindow::Timeout(BlockHeight(32)),
    })
    .unwrap();
    let old_contract: APINodeContract = serde_json::from_value(old_json).unwrap();
    assert_eq!(old_contract.contract_name.0, "my_contract");
    assert_eq!(old_contract.state_block_height.0, 0); // Default value
    assert_eq!(old_contract.state_commitment.0, vec![1, 2, 3]);
    assert_eq!(old_contract.program_id.0, vec![4, 5, 6]);
    assert_eq!(old_contract.verifier.0, "verifier1");
    assert_eq!(old_contract.timeout_window, Some(32));
    // Test new format
    let new_json = serde_json::to_value(&APINodeContract {
        contract_name: ContractName("new_contract".to_string()),
        state_block_height: BlockHeight(42),
        state_commitment: StateCommitment(vec![7, 8, 9]),
        program_id: ProgramId(vec![10, 11, 12]),
        verifier: Verifier("verifier2".to_string()),
        timeout_window: Some(123),
    })
    .unwrap();
    let new_contract: APINodeContract = serde_json::from_value(new_json).unwrap();
    assert_eq!(new_contract.contract_name.0, "new_contract");
    assert_eq!(new_contract.state_block_height.0, 42);
    assert_eq!(new_contract.state_commitment.0, vec![7, 8, 9]);
    assert_eq!(new_contract.program_id.0, vec![10, 11, 12]);
    assert_eq!(new_contract.verifier.0, "verifier2");
    assert_eq!(new_contract.timeout_window, Some(123));
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct APIContractState {
    // Struct for the contract_state table
    pub contract_name: String,             // Name of the contract
    pub block_hash: ConsensusProposalHash, // Hash of the block where the state is captured
    pub state_commitment: Vec<u8>,         // The contract state stored in JSON format
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct APIBlob {
    pub tx_hash: TxHash,       // Corresponds to the transaction hash
    pub blob_index: u32,       // Index of the blob within the transaction
    pub identity: String,      // Identity of the blob
    pub contract_name: String, // Contract name associated with the blob
    #[serde_as(as = "serde_with::hex::Hex")]
    pub data: Vec<u8>, // Actual blob data
    pub proof_outputs: Vec<serde_json::Value>, // outputs of proofs
    pub verified: bool,        // Verification status
}
