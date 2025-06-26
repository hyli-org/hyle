use borsh::{BorshDeserialize, BorshSerialize};
use sdk::*;
use serde::Serialize;
use std::collections::{BTreeSet, HashSet};
use std::collections::{HashMap, VecDeque};

// struct used to guarantee coherence between the 2 fields
#[derive(Default, Debug, Clone, BorshSerialize, BorshDeserialize, Serialize)]
pub struct OrderedTxMap {
    map: HashMap<TxHash, UnsettledBlobTransaction>,
    tx_order: HashMap<ContractName, VecDeque<TxHash>>,
}

impl OrderedTxMap {
    pub fn get(&self, hash: &TxHash) -> Option<&UnsettledBlobTransaction> {
        self.map.get(hash)
    }

    pub fn get_mut(&mut self, hash: &TxHash) -> Option<&mut UnsettledBlobTransaction> {
        self.map.get_mut(hash)
    }

    /// Returns true if the tx is the next to settle for all the contracts it contains
    pub fn is_next_to_settle(&self, tx_hash: &TxHash) -> bool {
        if let Some(unsettled_blob_tx) = self.map.get(tx_hash) {
            Self::get_contracts_blocked_by_tx(unsettled_blob_tx)
                .iter()
                .all(|contract_name| self.get_next_unsettled_tx(contract_name) == Some(tx_hash))
        } else {
            false
        }
    }

    pub fn get_next_unsettled_tx(&self, contract: &ContractName) -> Option<&TxHash> {
        self.tx_order.get(contract).and_then(|v| v.front())
    }

    pub fn get_earliest_unsettled_height(&self, contract: &ContractName) -> Option<BlockHeight> {
        self.tx_order
            .get(contract)
            .and_then(|v| v.front())
            .and_then(|tx_hash| self.map.get(tx_hash))
            .map(|tx| tx.tx_context.block_height)
    }

    pub fn get_for_settlement(
        &mut self,
        hash: &TxHash,
    ) -> Option<(&mut UnsettledBlobTransaction, bool)> {
        let tx = self.map.get_mut(hash);
        match tx {
            Some(tx) => {
                // Duplicates logic for efficiency / borrow-checker.
                let is_next_unsettled_tx =
                    Self::get_contracts_blocked_by_tx(tx)
                        .iter()
                        .all(|contract_name| {
                            self.tx_order.get(contract_name).and_then(|v| v.front()) == Some(hash)
                        });

                Some((tx, is_next_unsettled_tx))
            }
            None => None,
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn get_tx_order(&self, contract: &ContractName) -> Option<&VecDeque<TxHash>> {
        self.tx_order.get(contract)
    }

    pub fn get_contracts_blocked_by_tx(tx: &UnsettledBlobTransaction) -> HashSet<ContractName> {
        // Collect into a hashset for unicity
        let mut contract_names = HashSet::new();
        for blob in tx.blobs.values() {
            contract_names.insert(blob.blob.contract_name.clone());
            // This is a bit horrible, we should try to be leaner.
            if let Ok(data) =
                StructuredBlobData::<RegisterContractAction>::try_from(blob.blob.data.clone())
            {
                contract_names.insert(data.parameters.contract_name.clone());
            } else if let Ok(data) =
                StructuredBlobData::<DeleteContractAction>::try_from(blob.blob.data.clone())
            {
                contract_names.insert(data.parameters.contract_name.clone());
            }
        }
        contract_names
    }

    pub fn get_next_txs_blocked_by_tx(&self, tx: &UnsettledBlobTransaction) -> BTreeSet<TxHash> {
        let mut blocked_txs = BTreeSet::new();
        for contract in Self::get_contracts_blocked_by_tx(tx) {
            if let Some(next_tx) = self.get_next_unsettled_tx(&contract) {
                blocked_txs.insert(next_tx.clone());
            }
        }
        blocked_txs
    }

    /// Returns true if the tx is the next unsettled tx for all the contracts it contains
    /// If the TX was already in the map, this returns None
    pub fn add(&mut self, tx: UnsettledBlobTransaction) -> Option<bool> {
        if self.map.contains_key(&tx.hash) {
            return None;
        }
        let mut is_next = true;

        // Collect into a hashset for unicity
        let contract_names = Self::get_contracts_blocked_by_tx(&tx);
        for contract in contract_names {
            is_next = match self.tx_order.get_mut(&contract) {
                Some(vec) => {
                    vec.push_back(tx.hash.clone());
                    vec.len() == 1
                }
                None => {
                    self.tx_order.insert(contract.clone(), {
                        let mut vec = VecDeque::new();
                        vec.push_back(tx.hash.clone());
                        vec
                    });
                    true
                }
            } && is_next;
        }

        self.map.insert(tx.hash.clone(), tx);
        Some(is_next)
    }

    pub fn remove(&mut self, hash: &TxHash) -> Option<UnsettledBlobTransaction> {
        self.map.remove(hash).inspect(|tx| {
            // Remove the tx from the tx_order
            let contract_names = Self::get_contracts_blocked_by_tx(tx);
            for contract_name in contract_names {
                if let Some(vec) = self.tx_order.get_mut(&contract_name) {
                    vec.retain(|h| h != &tx.hash);
                    if vec.is_empty() {
                        self.tx_order.remove(&contract_name);
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    use std::collections::BTreeMap;

    use sdk::*;

    use super::*;

    fn new_tx(hash: &str, contract: &str) -> UnsettledBlobTransaction {
        UnsettledBlobTransaction {
            identity: Identity::new("toto"),
            hash: TxHash::new(hash),
            parent_dp_hash: DataProposalHash::default(),
            blobs_hash: BlobsHashes::default(),
            blobs: BTreeMap::from_iter(vec![(
                BlobIndex(0),
                UnsettledBlobMetadata {
                    blob: Blob {
                        contract_name: ContractName(contract.to_string()),
                        data: BlobData::default(),
                    },
                    possible_proofs: vec![],
                },
            )]),
            tx_context: TxContext::default(),
        }
    }

    fn is_next_unsettled_tx(map: &mut OrderedTxMap, hash: &TxHash) -> bool {
        let Some((_, is_next)) = map.get_for_settlement(hash) else {
            return false;
        };
        is_next
    }

    #[test]
    fn can_add_tx() {
        let mut map = OrderedTxMap::default();
        let tx = new_tx("tx1", "contract1");

        map.add(tx);
        assert_eq!(map.map.len(), 1);
        assert_eq!(map.tx_order.len(), 1);
    }

    #[test]
    fn can_get_tx() {
        let mut map = OrderedTxMap::default();
        let tx1 = TxHash::new("tx1");
        let tx2 = TxHash::new("tx2");
        let tx3 = TxHash::new("tx3");

        map.add(new_tx("tx1", "c1"));
        map.add(new_tx("tx2", "c1"));
        map.add(new_tx("tx3", "c2"));

        assert_eq!(tx1, map.get(&tx1).unwrap().hash);
        assert_eq!(tx2, map.get(&tx2).unwrap().hash);
        assert_eq!(tx3, map.get(&tx3).unwrap().hash);

        assert_eq!(map.map.len(), 3);
        assert_eq!(map.tx_order.len(), 2);
    }

    #[test]
    fn double_add_ignored() {
        let mut map = OrderedTxMap::default();
        let tx1 = TxHash::new("tx1");

        map.add(new_tx("tx1", "c1"));
        map.add(new_tx("tx1", "c1"));

        assert_eq!(tx1, map.get(&tx1).unwrap().hash);

        assert_eq!(map.map.len(), 1);
        assert_eq!(map.tx_order.len(), 1);
        assert_eq!(map.tx_order[&ContractName::new("c1")].len(), 1);
    }

    #[test]
    fn add_double_contract_name() {
        let mut map = OrderedTxMap::default();

        let mut tx = new_tx("tx1", "c1");
        tx.blobs
            .insert(BlobIndex(1), tx.blobs[&BlobIndex(0)].clone());
        tx.blobs
            .insert(BlobIndex(2), tx.blobs[&BlobIndex(0)].clone());
        tx.blobs.get_mut(&BlobIndex(1)).unwrap().blob.contract_name = ContractName::new("c2");

        let hash = tx.hash.clone();

        assert!(map.add(tx).unwrap());
        assert_eq!(
            map.tx_order.get(&"c1".into()),
            Some(&VecDeque::from_iter(vec![hash.clone()]))
        );
        assert_eq!(
            map.tx_order.get(&"c2".into()),
            Some(&VecDeque::from_iter(vec![hash]))
        );
    }

    #[test]
    fn check_next_unsettled_tx() {
        let mut map = OrderedTxMap::default();
        let tx1 = TxHash::new("tx1");
        let tx2 = TxHash::new("tx2");
        let tx3 = TxHash::new("tx3");
        let tx4 = TxHash::new("tx4");

        map.add(new_tx("tx1", "c1"));
        map.add(new_tx("tx2", "c1"));
        map.add(new_tx("tx3", "c2"));

        assert!(is_next_unsettled_tx(&mut map, &tx1));
        assert!(!is_next_unsettled_tx(&mut map, &tx2));
        assert!(is_next_unsettled_tx(&mut map, &tx3));
        // tx doesn't even exit
        assert!(!is_next_unsettled_tx(&mut map, &tx4));
    }

    #[test]
    fn remove_tx() {
        let mut map = OrderedTxMap::default();
        let tx1 = TxHash::new("tx1");
        let tx2 = TxHash::new("tx2");
        let tx3 = TxHash::new("tx3");
        let c1 = ContractName::new("c1");

        map.add(new_tx("tx1", "c1"));
        map.add(new_tx("tx2", "c1"));
        map.add(new_tx("tx3", "c2"));
        map.remove(&tx1);

        assert!(!is_next_unsettled_tx(&mut map, &tx1));
        assert!(is_next_unsettled_tx(&mut map, &tx2));
        assert!(is_next_unsettled_tx(&mut map, &tx3));

        assert_eq!(map.map.len(), 2);
        assert_eq!(map.tx_order.len(), 2);
        assert_eq!(map.tx_order[&c1].len(), 1);

        map.remove(&tx2);
        assert_eq!(map.map.len(), 1);
        assert_eq!(map.tx_order.len(), 1);
        assert_eq!(map.tx_order.get(&c1), None);
    }

    #[test]
    fn test_remove_extra_contract() {
        let mut map = OrderedTxMap::default();
        let contract1 = ContractName::new("c1");
        let contract2 = ContractName::new("c2");

        let mut tx = new_tx("tx1", "c1");
        tx.blobs.insert(
            BlobIndex(1),
            UnsettledBlobMetadata {
                blob: RegisterContractAction {
                    verifier: "test".into(),
                    contract_name: contract2.clone(),
                    program_id: ProgramId(vec![1, 2, 3]),
                    state_commitment: StateCommitment(vec![0, 1, 2, 3]),
                    ..Default::default()
                }
                .as_blob(contract1.clone(), None, None),
                possible_proofs: vec![],
            },
        );
        map.add(tx.clone());

        // Verify initial state
        assert_eq!(map.tx_order.get(&contract1).unwrap().len(), 1);
        assert_eq!(map.tx_order.get(&contract2).unwrap().len(), 1);

        map.remove(&tx.hash);

        assert_eq!(map.map.len(), 0);
        assert_eq!(map.tx_order.get(&contract1), None);
        assert_eq!(map.tx_order.get(&contract2), None);
    }
}
