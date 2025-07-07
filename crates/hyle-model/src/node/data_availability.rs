use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use utoipa::ToSchema;

use crate::{utils::TimestampMs, *};

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub enum DataEvent {
    OrderedSignedBlock(SignedBlock),
}

#[derive(
    Default, Debug, Clone, Serialize, Deserialize, ToSchema, BorshSerialize, BorshDeserialize,
)]
pub struct Contract {
    pub name: ContractName,
    pub program_id: ProgramId,
    pub state: StateCommitment,
    pub verifier: Verifier,
    pub timeout_window: TimeoutWindow,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    ToSchema,
)]
pub struct UnsettledBlobTransaction {
    pub identity: Identity,
    pub parent_dp_hash: DataProposalHash,
    pub hash: TxHash,
    pub tx_context: TxContext,
    pub blobs_hash: BlobsHashes,
    pub blobs: BTreeMap<BlobIndex, UnsettledBlobMetadata>,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    ToSchema,
)]
pub struct UnsettledBlobMetadata {
    pub blob: Blob,
    // Each time we receive a proof, we add it to this list
    pub possible_proofs: Vec<(ProgramId, HyleOutput)>,
}

#[derive(
    Debug,
    Default,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Eq,
    PartialEq,
)]
pub struct HandledBlobProofOutput {
    pub proof_tx_hash: TxHash,
    pub blob_tx_hash: TxHash,
    pub blob_index: BlobIndex,
    pub contract_name: ContractName,
    pub hyle_output: HyleOutput,
    pub blob_proof_output_index: usize,
}

#[derive(
    Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
pub struct BlobProofOutput {
    // TODO: this can be recovered from the hyle output
    pub blob_tx_hash: TxHash,
    // TODO: remove this?
    pub original_proof_hash: ProofDataHash,

    /// HyleOutput of the proof for this blob
    pub hyle_output: HyleOutput,
    /// Program ID used to verify the proof.
    pub program_id: ProgramId,
}

pub struct BlobProofOutputHash(pub Vec<u8>);

impl Hashed<BlobProofOutputHash> for BlobProofOutput {
    fn hashed(&self) -> BlobProofOutputHash {
        let mut hasher = Sha3_256::new();
        hasher.update(self.blob_tx_hash.0.as_bytes());
        hasher.update(self.original_proof_hash.0.as_bytes());
        hasher.update(self.program_id.0.clone());
        hasher.update(contract::Hashed::hashed(&self.hyle_output).0);
        BlobProofOutputHash(hasher.finalize().to_vec())
    }
}

pub struct HyleOutputHash(pub Vec<u8>);
impl Hashed<HyleOutputHash> for HyleOutput {
    fn hashed(&self) -> HyleOutputHash {
        let mut hasher = Sha3_256::new();
        hasher.update(self.version.to_le_bytes());
        hasher.update(self.initial_state.0.clone());
        hasher.update(self.next_state.0.clone());
        hasher.update(self.identity.0.as_bytes());
        hasher.update(self.index.0.to_le_bytes());
        for blob in &self.blobs {
            hasher.update(blob.0 .0.to_le_bytes());
            hasher.update(blob.1.contract_name.0.as_bytes());
            hasher.update(blob.1.data.0.as_slice());
        }
        hasher.update([self.success as u8]);
        hasher.update(self.onchain_effects.len().to_le_bytes());
        self.onchain_effects.iter().for_each(|c| match c {
            OnchainEffect::RegisterContract(c) => hasher.update(contract::Hashed::hashed(c).0),
            OnchainEffect::DeleteContract(cn) => hasher.update(cn.0.as_bytes()),
        });
        hasher.update(&self.program_outputs);
        HyleOutputHash(hasher.finalize().to_vec())
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, ToSchema, BorshSerialize, BorshDeserialize, Eq, PartialEq,
)]
#[serde(tag = "name", content = "metadata")]
pub enum TransactionStateEvent {
    Sequenced,
    Error(String),
    NewProof {
        blob_index: BlobIndex,
        proof_tx_hash: TxHash,
        program_output: Vec<u8>,
    },
    SettleEvent(String),
    Settled,
    SettledAsFailed,
    TimedOut,
    DroppedAsDuplicate,
}

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize)]
pub enum NodeStateEvent {
    NewBlock(Box<Block>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NodeStateIndexerEvent {
    DataProposalsFromBlock {
        block_hash: ConsensusProposalHash,
        block_height: BlockHeight,
        block_timestamp: TimestampMs,
        data_proposals: Vec<DataProposalMetadata>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataProposalMetadata {
    pub hash: DataProposalHash,
    pub parent_hash: Option<DataProposalHash>,
    pub lane_id: LaneId,
    pub tx_count: usize,
    pub estimated_size: usize,
    pub tx_hashes: Vec<TxHash>,
}
