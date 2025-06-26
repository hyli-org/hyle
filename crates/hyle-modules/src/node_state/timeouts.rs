use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};
use sdk::{BlockHeight, TxHash};
use serde::Serialize;

#[derive(Default, Debug, Clone, BorshSerialize, BorshDeserialize, Serialize)]
pub struct Timeouts {
    by_block: HashMap<BlockHeight, Vec<TxHash>>,
}

impl Timeouts {
    pub fn drop(&mut self, at: &BlockHeight) -> Vec<TxHash> {
        self.by_block.remove(at).unwrap_or_default()
    }

    /// Set timeout for a tx.
    /// This does not check if the TX is already set to timeout at a different (or same) block.
    pub fn set(&mut self, tx: TxHash, block_height: BlockHeight, timeout_window: BlockHeight) {
        self.by_block
            .entry(block_height + timeout_window)
            .or_default()
            .push(tx);
    }

    pub fn count_all(&self) -> usize {
        self.by_block.values().map(|v| v.len()).sum()
    }
}

#[cfg(any(test, feature = "test"))]
#[allow(unused)]
pub mod tests {
    use super::*;

    fn list_timeouts(t: &Timeouts, at: BlockHeight) -> Option<&Vec<TxHash>> {
        t.by_block.get(&at)
    }

    pub fn get(t: &Timeouts, tx: &TxHash) -> Option<BlockHeight> {
        t.by_block.iter().find_map(|(k, v)| {
            if v.contains(tx) {
                Some(k).copied()
            } else {
                None
            }
        })
    }

    #[test]
    fn timeout() {
        let mut t = Timeouts::default();
        let b1 = BlockHeight(0);
        let b2 = BlockHeight(1);
        let tx1 = TxHash::new("tx1");
        let window = BlockHeight(100);

        t.set(tx1.clone(), b1, window);

        assert_eq!(list_timeouts(&t, b1 + window).unwrap().len(), 1);
        assert_eq!(list_timeouts(&t, b2 + window), None);
        assert_eq!(get(&t, &tx1), Some(b1 + window));

        t.set(tx1.clone(), b2, window);

        assert_eq!(t.drop(&(b1 + window)), vec![tx1.clone()]);

        // Now this returns b2
        assert_eq!(get(&t, &tx1), Some(b2 + window));
        assert_eq!(list_timeouts(&t, b1 + window), None);
        assert_eq!(list_timeouts(&t, b2 + window).unwrap().len(), 1);

        assert_eq!(t.drop(&(b2 + window)), vec![tx1.clone()]);
        assert_eq!(get(&t, &tx1), None);
        assert_eq!(list_timeouts(&t, b2 + window), None);
    }
}
