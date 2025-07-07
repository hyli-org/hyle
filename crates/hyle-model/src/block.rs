use std::{cmp::Ordering, collections::BTreeMap};

use anyhow::Context;
use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};
use derive_more::derive::Display;
use serde::{Deserialize, Serialize};
use utils::TimestampMs;

use crate::{staking::*, *};

#[derive(
    Debug, Default, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Eq, PartialEq,
)]
pub struct Block {
    pub parent_hash: ConsensusProposalHash,
    pub hash: ConsensusProposalHash,
    pub block_height: BlockHeight,
    pub block_timestamp: TimestampMs,
    pub txs: Vec<(TxId, Transaction)>,
    pub dp_parent_hashes: BTreeMap<TxHash, DataProposalHash>,
    pub lane_ids: BTreeMap<TxHash, LaneId>,
    pub successful_txs: Vec<TxHash>,
    pub failed_txs: Vec<TxHash>,
    pub timed_out_txs: Vec<TxHash>,
    pub dropped_duplicate_txs: Vec<TxId>,
    pub blob_proof_outputs: Vec<HandledBlobProofOutput>,
    pub verified_blobs: Vec<(TxHash, BlobIndex, Option<usize>)>,
    pub new_bounded_validators: Vec<ValidatorPublicKey>,
    pub staking_actions: Vec<(Identity, StakingAction)>,
    pub registered_contracts:
        BTreeMap<ContractName, (TxHash, RegisterContractEffect, Option<Vec<u8>>)>,
    pub deleted_contracts: BTreeMap<ContractName, TxHash>,
    pub updated_states: BTreeMap<ContractName, StateCommitment>,
    pub updated_program_ids: BTreeMap<ContractName, ProgramId>,
    pub updated_timeout_windows: BTreeMap<ContractName, TimeoutWindow>,
    pub transactions_events: BTreeMap<TxHash, Vec<TransactionStateEvent>>,
}

impl Block {
    pub fn total_txs(&self) -> usize {
        self.txs.len()
    }

    pub fn resolve_parent_dp_hash(&self, tx_hash: &TxHash) -> Result<DataProposalHash> {
        Ok(self
            .dp_parent_hashes
            .get(tx_hash)
            .context(format!("No parent dp hash found for tx {tx_hash}"))?
            .clone())
    }

    pub fn resolve_lane_id(&self, tx_hash: &TxHash) -> Result<LaneId> {
        Ok(self
            .lane_ids
            .get(tx_hash)
            .context(format!("No lane id found for tx {tx_hash}"))?
            .clone())
    }

    pub fn build_tx_ctx(&self, tx_hash: &TxHash) -> Result<TxContext> {
        let lane_id = self.resolve_lane_id(tx_hash)?;

        Ok(TxContext {
            lane_id,
            block_hash: self.hash.clone(),
            block_height: self.block_height,
            timestamp: self.block_timestamp.clone(),
            chain_id: HYLE_TESTNET_CHAIN_ID, // TODO: make it configurable
        })
    }
}

impl Ord for Block {
    fn cmp(&self, other: &Self) -> Ordering {
        self.block_height.0.cmp(&other.block_height.0)
    }
}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Display)]
#[display("")]
pub struct SignedBlock {
    pub data_proposals: Vec<(LaneId, Vec<DataProposal>)>,
    pub consensus_proposal: ConsensusProposal,
    // NB: this can be different for different validators
    pub certificate: AggregateSignature,
}

impl SignedBlock {
    pub fn parent_hash(&self) -> &ConsensusProposalHash {
        &self.consensus_proposal.parent_hash
    }

    pub fn height(&self) -> BlockHeight {
        BlockHeight(self.consensus_proposal.slot)
    }

    pub fn has_txs(&self) -> bool {
        for (_, _, txs) in self.iter_txs() {
            if !txs.is_empty() {
                return true;
            }
        }

        false
    }

    pub fn count_txs(&self) -> usize {
        self.iter_txs().map(|(_, _, txs)| txs.len()).sum()
    }

    pub fn iter_txs(&self) -> impl Iterator<Item = (LaneId, DataProposalHash, &Vec<Transaction>)> {
        self.data_proposals
            .iter()
            .flat_map(|(lane_id, dps)| std::iter::zip(std::iter::repeat(lane_id.clone()), dps))
            .map(|(lane_id, dp)| {
                (
                    lane_id.clone(),
                    dp.parent_data_proposal_hash
                        .clone()
                        // This is weird but has to match the workaround in own_lane.rs
                        .unwrap_or(DataProposalHash(hex::encode(lane_id.0 .0))),
                    &dp.txs,
                )
            })
    }

    pub fn iter_txs_with_id(&self) -> impl Iterator<Item = (LaneId, TxId, &Transaction)> {
        self.iter_txs().flat_map(move |(lane_id, dp_hash, txs)| {
            txs.iter()
                .map(move |tx| (lane_id.clone(), TxId(dp_hash.clone(), tx.hashed()), tx))
        })
    }

    pub fn extract_data_proposal_metadata(&self) -> Vec<DataProposalMetadata> {
        let mut data_proposals_metadata = Vec::new();

        for (lane_id, data_proposals) in &self.data_proposals {
            for dp in data_proposals {
                let dp_hash = dp.hashed();
                let tx_hashes: Vec<TxHash> = dp.txs.iter().map(|tx| tx.hashed()).collect();

                let parent_hash: Option<DataProposalHash> = if self.height().0 == 0 {
                    Some(DataProposalHash(hex::encode(&lane_id.0 .0)))
                } else {
                    dp.parent_data_proposal_hash.clone()
                };

                data_proposals_metadata.push(DataProposalMetadata {
                    hash: dp_hash,
                    parent_hash,
                    lane_id: lane_id.clone(),
                    tx_count: dp.txs.len(),
                    estimated_size: dp.estimate_size(),
                    tx_hashes,
                });
            }
        }
        data_proposals_metadata
    }
}

impl Hashed<ConsensusProposalHash> for SignedBlock {
    fn hashed(&self) -> ConsensusProposalHash {
        self.consensus_proposal.hashed()
    }
}

impl Ord for SignedBlock {
    fn cmp(&self, other: &Self) -> Ordering {
        self.height().0.cmp(&other.height().0)
    }
}

impl PartialOrd for SignedBlock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SignedBlock {
    fn eq(&self, other: &Self) -> bool {
        self.hashed() == other.hashed()
    }
}

impl Eq for SignedBlock {}

impl std::default::Default for SignedBlock {
    fn default() -> Self {
        SignedBlock {
            consensus_proposal: ConsensusProposal::default(),
            data_proposals: vec![],
            certificate: AggregateSignature {
                signature: crate::Signature("signature".into()),
                validators: vec![],
            },
        }
    }
}
