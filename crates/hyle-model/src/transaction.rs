use std::collections::BTreeMap;
use std::sync::RwLock;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use strum::IntoDiscriminant;
use strum_macros::{EnumDiscriminants, IntoStaticStr};
use utoipa::{
    openapi::{ArrayBuilder, ObjectBuilder, RefOr, Schema},
    PartialSchema, ToSchema,
};

use crate::{api::APIRegisterContract, *};

#[derive(
    Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
pub struct Transaction {
    pub version: u32,
    pub transaction_data: TransactionData,
}

impl Transaction {
    pub fn metadata(&self, parent_data_proposal_hash: DataProposalHash) -> TransactionMetadata {
        TransactionMetadata {
            version: self.version,
            transaction_kind: self.transaction_data.discriminant(),
            id: TxId(parent_data_proposal_hash, self.hashed()),
        }
    }
}

impl DataSized for Transaction {
    fn estimate_size(&self) -> usize {
        match &self.transaction_data {
            TransactionData::Blob(tx) => tx.estimate_size(),
            TransactionData::Proof(tx) => tx.estimate_size(),
            TransactionData::VerifiedProof(tx) => tx.proof_size,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct TransactionMetadata {
    pub version: u32,
    pub transaction_kind: TransactionKind,
    pub id: TxId,
}

#[derive(
    EnumDiscriminants,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    IntoStaticStr,
)]
#[strum_discriminants(derive(Default, BorshSerialize, BorshDeserialize))]
#[strum_discriminants(name(TransactionKind))]
pub enum TransactionData {
    #[strum_discriminants(default)]
    Blob(BlobTransaction),
    Proof(ProofTransaction),
    VerifiedProof(VerifiedProofTransaction),
}

impl Default for TransactionData {
    fn default() -> Self {
        TransactionData::Blob(BlobTransaction::default())
    }
}

#[derive(
    Serialize,
    Deserialize,
    ToSchema,
    Default,
    PartialEq,
    Eq,
    Clone,
    BorshSerialize,
    BorshDeserialize,
)]
pub struct ProofTransaction {
    pub contract_name: ContractName,
    pub proof: ProofData,
}

impl ProofTransaction {
    pub fn estimate_size(&self) -> usize {
        borsh::to_vec(self).unwrap_or_default().len()
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct VerifiedProofTransaction {
    pub contract_name: ContractName,
    pub proof: Option<ProofData>, // Kept only on the local lane for indexing purposes
    pub proof_hash: ProofDataHash,
    pub proof_size: usize,
    pub proven_blobs: Vec<BlobProofOutput>,
    pub is_recursive: bool,
}

impl std::fmt::Debug for VerifiedProofTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedProofTransaction")
            .field("contract_name", &self.contract_name)
            .field("proof_hash", &self.proof_hash)
            .field("proof_size", &self.proof_size)
            .field("proof", &"[HIDDEN]")
            .field(
                "proof_len",
                &match &self.proof {
                    Some(v) => v.0.len(),
                    None => 0,
                },
            )
            .field("proven_blobs", &self.proven_blobs)
            .finish()
    }
}

impl std::fmt::Debug for ProofTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProofTransaction")
            .field("contract_name", &self.contract_name)
            .field("proof", &"[HIDDEN]")
            .field("proof_len", &self.proof.0.len())
            .finish()
    }
}

impl Transaction {
    pub fn wrap(data: TransactionData) -> Self {
        Transaction {
            version: 1,
            transaction_data: data,
        }
    }
}

impl From<TransactionData> for Transaction {
    fn from(data: TransactionData) -> Self {
        Transaction::wrap(data)
    }
}

impl From<BlobTransaction> for Transaction {
    fn from(tx: BlobTransaction) -> Self {
        Transaction::wrap(TransactionData::Blob(tx))
    }
}

impl From<ProofTransaction> for Transaction {
    fn from(tx: ProofTransaction) -> Self {
        Transaction::wrap(TransactionData::Proof(tx))
    }
}

impl From<VerifiedProofTransaction> for Transaction {
    fn from(tx: VerifiedProofTransaction) -> Self {
        Transaction::wrap(TransactionData::VerifiedProof(tx))
    }
}

impl Hashed<TxHash> for Transaction {
    fn hashed(&self) -> TxHash {
        match &self.transaction_data {
            TransactionData::Blob(tx) => tx.hashed(),
            TransactionData::Proof(tx) => tx.hashed(),
            TransactionData::VerifiedProof(tx) => tx.hashed(),
        }
    }
}

impl Hashed<TxHash> for ProofTransaction {
    fn hashed(&self) -> TxHash {
        let mut hasher = Sha3_256::new();
        hasher.update(self.contract_name.0.as_bytes());
        hasher.update(self.proof.hashed().0);
        let hash_bytes = hasher.finalize();
        TxHash(hex::encode(hash_bytes))
    }
}
impl Hashed<TxHash> for VerifiedProofTransaction {
    fn hashed(&self) -> TxHash {
        let mut hasher = Sha3_256::new();
        hasher.update(self.contract_name.0.as_bytes());
        hasher.update(self.proof_hash.0.as_bytes());
        let hash_bytes = hasher.finalize();
        TxHash(hex::encode(hash_bytes))
    }
}

#[derive(Serialize, Deserialize, Default, BorshSerialize, BorshDeserialize)]
#[readonly::make]
pub struct BlobTransaction {
    pub identity: Identity,
    pub blobs: Vec<Blob>,
    // FIXME: add a nonce or something to prevent BlobTransaction to share the same hash
    #[borsh(skip)]
    #[serde(skip_serializing, skip_deserializing)]
    hash_cache: RwLock<Option<TxHash>>,
    #[borsh(skip)]
    #[serde(skip_serializing, skip_deserializing)]
    blobshash_cache: RwLock<Option<BlobsHashes>>,
}

impl BlobTransaction {
    pub fn new(identity: impl Into<Identity>, blobs: Vec<Blob>) -> Self {
        BlobTransaction {
            identity: identity.into(),
            blobs,
            hash_cache: RwLock::new(None),
            blobshash_cache: RwLock::new(None),
        }
    }
}

// Custom implem to skip the cached fields
impl std::fmt::Debug for BlobTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobTransaction")
            .field("identity", &self.identity)
            .field("blobs", &self.blobs)
            .finish()
    }
}

impl PartialSchema for BlobTransaction {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("identity", Identity::schema())
                .property("blobs", ArrayBuilder::new().items(Blob::schema()).build())
                .required("identity")
                .required("blobs")
                .build(),
        ))
    }
}

impl ToSchema for BlobTransaction {}

impl Clone for BlobTransaction {
    fn clone(&self) -> Self {
        BlobTransaction {
            identity: self.identity.clone(),
            blobs: self.blobs.clone(),
            hash_cache: RwLock::new(self.hash_cache.read().unwrap().clone()),
            blobshash_cache: RwLock::new(self.blobshash_cache.read().unwrap().clone()),
        }
    }
}

impl PartialEq for BlobTransaction {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity && self.blobs == other.blobs
    }
}

impl Eq for BlobTransaction {}

impl BlobTransaction {
    pub fn estimate_size(&self) -> usize {
        borsh::to_vec(self).unwrap_or_default().len()
    }
}

impl Hashed<TxHash> for BlobTransaction {
    fn hashed(&self) -> TxHash {
        if let Some(hash) = self.hash_cache.read().unwrap().clone() {
            return hash;
        }
        let mut hasher = Sha3_256::new();
        hasher.update(self.identity.0.as_bytes());
        for blob in self.blobs.iter() {
            hasher.update(blob.hashed().0);
        }
        let hash_bytes = hasher.finalize();
        let tx_hash = TxHash(hex::encode(hash_bytes));
        *self.hash_cache.write().unwrap() = Some(tx_hash.clone());
        tx_hash
    }
}

impl BlobTransaction {
    pub fn blobs_hash(&self) -> BlobsHashes {
        if let Some(hash) = self.blobshash_cache.read().unwrap().clone() {
            return hash;
        }
        let hash: BlobsHashes = (&self.blobs).into();
        self.blobshash_cache.write().unwrap().replace(hash.clone());
        hash
    }

    pub fn validate_identity(&self) -> Result<(), anyhow::Error> {
        // Checks that there is a blob that proves the identity
        let Some((identity, identity_contract_name)) = self.identity.0.rsplit_once("@") else {
            anyhow::bail!("Transaction identity {} is not correctly formed. It should be in the form <id>@<contract_id_name>", self.identity.0);
        };

        if identity.is_empty() || identity_contract_name.is_empty() {
            anyhow::bail!(
                "Transaction identity {}@{} must not have empty parts",
                identity,
                identity_contract_name
            );
        }

        // Check that there is at least one blob that has identity_contract_name as contract name
        if !self
            .blobs
            .iter()
            .any(|blob| blob.contract_name.0 == identity_contract_name)
        {
            anyhow::bail!(
                "Can't find blob that proves the identity on contract '{}'",
                identity_contract_name
            );
        }
        Ok(())
    }
}

impl From<APIRegisterContract> for BlobTransaction {
    fn from(payload: APIRegisterContract) -> Self {
        BlobTransaction::new(
            "hyle@hyle",
            vec![RegisterContractAction {
                verifier: payload.verifier,
                program_id: payload.program_id,
                state_commitment: payload.state_commitment,
                contract_name: payload.contract_name,
                timeout_window: match payload.timeout_window {
                    Some(0) => Some(TimeoutWindow::NoTimeout),
                    Some(timeout) => Some(TimeoutWindow::Timeout(BlockHeight(timeout))),
                    None => None,
                },
                constructor_metadata: payload.constructor_metadata,
            }
            .as_blob("hyle".into(), None, None)],
        )
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    Serialize,
    Deserialize,
    ToSchema,
    Eq,
    PartialEq,
    Hash,
    BorshSerialize,
    BorshDeserialize,
)]
pub struct BlobsHashes {
    pub hashes: BTreeMap<BlobIndex, BlobHash>,
}

impl From<&Vec<Blob>> for BlobsHashes {
    fn from(iter: &Vec<Blob>) -> Self {
        BlobsHashes {
            hashes: iter
                .iter()
                .enumerate()
                .map(|(index, blob)| (BlobIndex(index), blob.hashed()))
                .collect(),
        }
    }
}

impl From<&IndexedBlobs> for BlobsHashes {
    fn from(iter: &IndexedBlobs) -> Self {
        BlobsHashes {
            hashes: iter
                .iter()
                .map(|(index, blob)| (*index, blob.hashed()))
                .collect(),
        }
    }
}

impl BlobsHashes {
    pub fn includes_all(&self, other: &BlobsHashes) -> bool {
        for (index, hash) in other.hashes.iter() {
            if !self
                .hashes
                .iter()
                .any(|(other_index, other_hash)| index == other_index && hash == other_hash)
            {
                return false;
            }
        }
        true
    }
}

impl std::fmt::Display for BlobsHashes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (BlobIndex(index), BlobHash(hash)) in self.hashes.iter() {
            write!(f, "[{index}]: {hash}")?;
        }
        Ok(())
    }
}
