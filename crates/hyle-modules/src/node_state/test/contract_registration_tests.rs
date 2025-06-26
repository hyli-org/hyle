#![cfg(test)]

use std::collections::HashSet;

use client_sdk::transaction_builder::ProvableBlobTx;
use hydentity::{
    client::tx_executor_handler::{register_identity, verify_identity},
    Hydentity, HydentityAction,
};

pub const HYLI_WALLET: &str = "wallet";
use crate::node_state::hyle_tld::HYLI_TLD_ID;

use super::*;

pub fn make_register_tx(
    sender: Identity,
    tld: ContractName,
    name: ContractName,
) -> BlobTransaction {
    BlobTransaction::new(
        sender,
        vec![RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: name,
            ..Default::default()
        }
        .as_blob(tld, None, None)],
    )
}

pub fn make_register_hyli_wallet_identity_tx() -> BlobTransaction {
    let mut tx = ProvableBlobTx::new(HYLI_TLD_ID.into());
    register_identity(&mut tx, HYLI_WALLET.into(), "password".into());
    BlobTransaction::new(HYLI_TLD_ID.to_string(), tx.blobs)
}

#[test_log::test(tokio::test)]
async fn test_register_contract_simple_hyle() {
    let mut state = new_node_state().await;

    let register_c1 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c1".into());
    let register_c2 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c2.hyle".into());
    let register_c3 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c3".into());

    state.craft_block_and_handle(1, vec![register_c1.clone().into()]);

    state.craft_block_and_handle(2, vec![register_c2.into(), register_c3.into()]);

    assert_eq!(
        state.contracts.keys().collect::<HashSet<_>>(),
        HashSet::from_iter(vec![
            &"hyle".into(),
            &"c1".into(),
            &"c2.hyle".into(),
            &"c3".into()
        ])
    );

    let block = state.craft_block_and_handle(3, vec![register_c1.clone().into()]);

    assert_eq!(block.failed_txs, vec![register_c1.hashed()]);
    assert_eq!(state.contracts.len(), 4);
}

#[test_log::test(tokio::test)]
async fn test_register_contract_failure() {
    let mut state = new_node_state().await;

    let register_1 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c1.hyle.lol".into());
    let register_2 = make_register_tx("other@hyle".into(), "hyle".into(), "c2.hyle.hyle".into());
    let register_3 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c3.other".into());
    let register_4 = make_register_tx("hyle@hyle".into(), "hyle".into(), ".hyle".into());
    let register_5 = BlobTransaction::new(
        "hyle@hyle",
        vec![Blob {
            contract_name: "hyle".into(),
            data: BlobData(vec![0, 1, 2, 3]),
        }],
    );
    let register_good = make_register_tx("hyle@hyle".into(), "hyle".into(), "c1.hyle".into());

    let signed_block = craft_signed_block(
        1,
        vec![
            register_1.clone().into(),
            register_2.clone().into(),
            register_3.clone().into(),
            register_4.clone().into(),
            register_5.clone().into(),
            register_good.clone().into(),
        ],
    );

    let block = state.force_handle_block(&signed_block);

    assert_eq!(state.contracts.len(), 2);
    assert_eq!(block.successful_txs, vec![register_good.hashed()]);
    assert_eq!(
        block.failed_txs,
        vec![
            register_1.hashed(),
            register_2.hashed(),
            register_3.hashed(),
            register_4.hashed(),
            register_5.hashed(),
        ]
    );
}

#[test_log::test(tokio::test)]
async fn test_register_contract_proof_mismatch() {
    let mut state = new_node_state().await;

    // Create a valid registration transaction
    let register_parent_tx =
        make_register_tx("hyle@hyle".into(), "hyle".into(), "test.hyle".into());
    let register_parent_tx_hash = register_parent_tx.hashed();
    let register_tx = make_register_tx(
        "hyle@test.hyle".into(),
        "test.hyle".into(),
        "sub.test.hyle".into(),
    );
    let tx_hash = register_tx.hashed();

    // Create a proof with mismatched registration effect
    let mut output = make_hyle_output(register_tx.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContract(RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![9, 9, 9, 9]), // Different state_commitment than in the blob action
            contract_name: "sub.test.hyle".into(),
            timeout_window: None,
        }));

    let proof_tx = new_proof_tx(&"test.hyle".into(), &output, &tx_hash);

    // Submit both transactions
    let block = state.craft_block_and_handle(
        1,
        vec![
            register_parent_tx.into(),
            register_tx.into(),
            proof_tx.into(),
        ],
    );

    // The transaction should fail because the proof's registration effect doesn't match the blob action
    tracing::warn!("{:?}", state.contracts);
    assert_eq!(state.contracts.len(), 2); // sub.test.hyle shouldn't exist
    assert_eq!(block.successful_txs, vec![register_parent_tx_hash]); // No successful transactions
}

#[test_log::test(tokio::test)]
async fn test_register_contract_composition() {
    let mut state = new_node_state().await;
    let register = make_register_tx("hyle@hyle".into(), "hyle".into(), "hydentity".into());
    let block = state.craft_block_and_handle(1, vec![register.clone().into()]);

    check_block_is_ok(&block);

    assert_eq!(state.contracts.len(), 2);

    let compositing_register_willfail = BlobTransaction::new(
        "test@hydentity",
        vec![
            RegisterContractAction {
                verifier: "test".into(),
                program_id: ProgramId(vec![]),
                state_commitment: StateCommitment(vec![0, 1, 2, 3]),
                contract_name: "c1".into(),
                ..Default::default()
            }
            .as_blob("hyle".into(), None, None),
            Blob {
                contract_name: "hydentity".into(),
                data: BlobData(vec![0, 1, 2, 3]),
            },
        ],
    );
    // Try to register the same contract validly later.
    // Change identity to change blob tx hash
    let compositing_register_good = BlobTransaction::new(
        "test2@hydentity",
        compositing_register_willfail.blobs.clone(),
    );

    let crafted_block = craft_signed_block(
        102,
        vec![
            compositing_register_willfail.clone().into(),
            compositing_register_good.clone().into(),
        ],
    );

    let block = state.force_handle_block(&crafted_block);
    assert_eq!(state.contracts.len(), 2);

    check_block_is_ok(&block);

    let proof_tx = new_proof_tx(
        &"hyle".into(),
        &make_hyle_output(compositing_register_good.clone(), BlobIndex(1)),
        &compositing_register_good.hashed(),
    );

    let block = state.craft_block_and_handle(103, vec![proof_tx.into()]);

    check_block_is_ok(&block);

    assert_eq!(state.contracts.len(), 2);

    // Send a third one that will fail early on settlement of the second because duplication
    // (and thus test the early-failure settlement path)

    let third_tx = BlobTransaction::new(
        "test3@hydentity",
        compositing_register_willfail.blobs.clone(),
    );
    let proof_tx = new_proof_tx(
        &"hyle".into(),
        &make_hyle_output(third_tx.clone(), BlobIndex(1)),
        &third_tx.hashed(),
    );

    let block = state.craft_block_and_handle(104, vec![third_tx.clone().into()]);

    check_block_is_ok(&block);

    assert_eq!(state.contracts.len(), 2);

    let mut block = state.craft_block_and_handle(105, vec![proof_tx.clone().into()]);

    check_block_is_ok(&block);

    let block = state.craft_block_and_handle(102 + 100, vec![]);

    check_block_is_ok(&block);

    assert_eq!(
        block.timed_out_txs,
        vec![compositing_register_willfail.hashed()]
    );
    assert_eq!(state.contracts.len(), 3);
}

fn check_block_is_ok(block: &Block) {
    let dp_hashes: Vec<TxHash> = block.dp_parent_hashes.clone().into_keys().collect();

    for tx_hash in block.successful_txs.iter() {
        assert!(dp_hashes.contains(tx_hash));
    }

    for tx_hash in block.failed_txs.iter() {
        assert!(dp_hashes.contains(tx_hash));
    }

    for tx_hash in block.timed_out_txs.iter() {
        assert!(dp_hashes.contains(tx_hash));
    }

    for (tx_hash, _) in block.transactions_events.iter() {
        assert!(dp_hashes.contains(tx_hash));
    }
}

pub fn make_delete_tx(
    sender: Identity,
    tld: ContractName,
    contract_name: ContractName,
) -> BlobTransaction {
    BlobTransaction::new(
        sender,
        vec![DeleteContractAction { contract_name }.as_blob(tld, None, None)],
    )
}

pub fn make_delete_tx_with_hyli(tld: ContractName, contract_name: ContractName) -> BlobTransaction {
    BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            DeleteContractAction { contract_name }.as_blob(tld, None, None),
        ],
    )
}

pub fn make_update_timeout_window_tx_with_hyli(
    tld: ContractName,
    contract_name: ContractName,
    timeout_window: TimeoutWindow,
) -> BlobTransaction {
    BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            UpdateContractTimeoutWindowAction {
                contract_name,
                timeout_window,
            }
            .as_blob(tld, None, None),
        ],
    )
}
#[test_log::test(tokio::test)]
async fn test_register_contract_and_delete_hyle() {
    let mut state = new_node_state().await;

    let register_wallet = make_register_tx("hyle@hyle".into(), "hyle".into(), "wallet".into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let mut output = make_hyle_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof =
        new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

    let register_c1 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c1".into());
    let register_c2 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c2.hyle".into());
    // This technically doesn't matter as it's actually the proof that does the work
    let register_sub_c2 = make_register_tx(
        "toto@c2.hyle".into(),
        "c2.hyle".into(),
        "sub.c2.hyle".into(),
    );

    let mut output = make_hyle_output(register_sub_c2.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContract(RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: "sub.c2.hyle".into(),
            timeout_window: None,
        }));
    let sub_c2_proof = new_proof_tx(&"c2.hyle".into(), &output, &register_sub_c2.hashed());

    let block = state.craft_block_and_handle(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof.into(),
            register_c1.into(),
            register_c2.into(),
            register_sub_c2.into(),
            sub_c2_proof.into(),
        ],
    );
    assert_eq!(
        block
            .registered_contracts
            .keys()
            .map(|cn| cn.0.clone())
            .collect::<Vec<_>>(),
        vec!["c1", "c2.hyle", "sub.c2.hyle", "wallet"]
    );
    assert_eq!(state.contracts.len(), 5);

    // Now delete them.
    let self_delete_tx = make_delete_tx("c1@c1".into(), "c1".into(), "c1".into());
    let delete_sub_tx = make_delete_tx(
        "toto@c2.hyle".into(),
        "c2.hyle".into(),
        "sub.c2.hyle".into(),
    );
    let delete_tx = make_delete_tx_with_hyli("hyle".into(), "c2.hyle".into());
    let mut output = make_hyle_output_bis(delete_tx.clone(), BlobIndex(0));
    let delete_tx_proof = new_proof_tx(&"c2.hyle".into(), &output, &delete_tx.hashed());

    let mut output = make_hyle_output(self_delete_tx.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("c1".into()));
    let delete_self_proof = new_proof_tx(&"c1.hyle".into(), &output, &self_delete_tx.hashed());

    let mut output =
        make_hyle_output_with_state(delete_sub_tx.clone(), BlobIndex(0), &[4, 5, 6], &[1]);
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("sub.c2.hyle".into()));
    let delete_sub_proof = new_proof_tx(&"c2.hyle".into(), &output, &delete_sub_tx.hashed());

    let block = state.craft_block_and_handle(
        2,
        vec![
            self_delete_tx.into(),
            delete_sub_tx.into(),
            delete_self_proof.into(),
            delete_sub_proof.into(),
            delete_tx.into(),
            delete_tx_proof.into(),
        ],
    );

    assert_eq!(
        block
            .deleted_contracts
            .keys()
            .map(|dce| dce.0.clone())
            .collect::<Vec<_>>(),
        vec!["c1", "c2.hyle", "sub.c2.hyle"]
    );
    assert_eq!(state.contracts.len(), 2);
}
#[test_log::test(tokio::test)]
async fn test_hyle_delete_contract_with_wrong_proof() {
    let mut state = new_node_state().await;
    let register_wallet = make_register_tx("hyle@hyle".into(), "hyle".into(), HYLI_WALLET.into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let mut output = make_hyle_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof = new_proof_tx(
        &HYLI_WALLET.into(),
        &output,
        &register_hyli_at_wallet.hashed(),
    );

    let register_contract = make_register_tx("hyle@hyle".into(), "hyle".into(), "contract".into());

    state.craft_block_and_handle(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof.into(),
            register_contract.into(),
        ],
    );

    assert_eq!(state.contracts.len(), 3);

    let delete_tx = make_delete_tx(HYLI_TLD_ID.into(), "hyle".into(), "contract".into());

    let mut output = make_hyle_output_bis(delete_tx.clone(), BlobIndex(0));
    output.success = false; // Simulate a wrong proof
    let verify_hyli_proof = new_proof_tx(&HYLI_WALLET.into(), &output, &delete_tx.hashed());

    let block =
        state.craft_block_and_handle(2, vec![delete_tx.into(), verify_hyli_proof.clone().into()]);

    assert_eq!(block.deleted_contracts.len(), 0);
    assert_eq!(state.contracts.len(), 3);
}

#[test_log::test(tokio::test)]
async fn test_hyle_delete_contract_with_wrong_identity() {
    let mut state = new_node_state().await;
    let register_wallet = make_register_tx("hyle@hyle".into(), "hyle".into(), HYLI_WALLET.into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let mut output = make_hyle_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof = new_proof_tx(
        &HYLI_WALLET.into(),
        &output,
        &register_hyli_at_wallet.hashed(),
    );

    let register_contract = make_register_tx("hyle@hyle".into(), "hyle".into(), "contract".into());

    state.craft_block_and_handle(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof.into(),
            register_contract.into(),
        ],
    );

    assert_eq!(state.contracts.len(), 3);

    let delete_tx = make_delete_tx(
        Identity::new("random@wallou"),
        "hyle".into(),
        "contract".into(),
    );

    let mut output = make_hyle_output_bis(delete_tx.clone(), BlobIndex(0));
    let verify_hyli_proof = new_proof_tx(&HYLI_WALLET.into(), &output, &delete_tx.hashed());

    let block =
        state.craft_block_and_handle(2, vec![delete_tx.into(), verify_hyli_proof.clone().into()]);

    assert_eq!(block.deleted_contracts.len(), 0);
    assert_eq!(state.contracts.len(), 3);
}

#[test_log::test(tokio::test)]
async fn test_hyle_delete_contract_success() {
    let mut state = new_node_state().await;
    let register_wallet = make_register_tx("hyle@hyle".into(), "hyle".into(), "wallet".into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let mut output = make_hyle_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof =
        new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

    let register_contract = make_register_tx("hyle@hyle".into(), "hyle".into(), "contract".into());

    state.craft_block_and_handle(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof.into(),
            register_contract.into(),
        ],
    );

    assert_eq!(state.contracts.len(), 3);

    let delete_tx = make_delete_tx_with_hyli("hyle".into(), "contract".into());

    let mut output = make_hyle_output_bis(delete_tx.clone(), BlobIndex(0));
    let verify_hyli_proof = new_proof_tx(&"wallet".into(), &output, &delete_tx.hashed());

    let block =
        state.craft_block_and_handle(2, vec![delete_tx.into(), verify_hyli_proof.clone().into()]);

    assert_eq!(block.deleted_contracts.len(), 1);
    assert_eq!(state.contracts.len(), 2);
}

#[test_log::test(tokio::test)]
async fn test_hyle_contract_update_timeout_window() {
    let mut state = new_node_state().await;
    let register_wallet = make_register_tx("hyle@hyle".into(), "hyle".into(), "wallet".into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let mut output = make_hyle_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof =
        new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

    let register_contract = make_register_tx("hyle@hyle".into(), "hyle".into(), "contract".into());

    state.craft_block_and_handle(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof.into(),
            register_contract.into(),
        ],
    );

    assert_eq!(state.contracts.len(), 3);
    assert_eq!(
        state
            .contracts
            .get(&ContractName::new("contract"))
            .unwrap()
            .timeout_window,
        TimeoutWindow::Timeout(BlockHeight(100))
    );

    let timeout_window_update_tx = make_update_timeout_window_tx_with_hyli(
        "hyle".into(),
        "contract".into(),
        TimeoutWindow::Timeout(BlockHeight(45)),
    );

    let mut output = make_hyle_output_bis(timeout_window_update_tx.clone(), BlobIndex(0));
    let verify_hyli_proof = new_proof_tx(
        &"wallet".into(),
        &output,
        &timeout_window_update_tx.hashed(),
    );

    let block = state.craft_block_and_handle(
        2,
        vec![
            timeout_window_update_tx.into(),
            verify_hyli_proof.clone().into(),
        ],
    );

    assert_eq!(block.deleted_contracts.len(), 0);
    assert_eq!(state.contracts.len(), 3);
    assert_eq!(
        state
            .contracts
            .get(&ContractName::new("contract"))
            .unwrap()
            .timeout_window,
        TimeoutWindow::Timeout(BlockHeight(45))
    );
}

#[test_log::test(tokio::test)]
async fn test_hyle_sub_delete() {
    let mut state = new_node_state().await;

    let register_wallet = make_register_tx("hyle@hyle".into(), "hyle".into(), "wallet".into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let mut output = make_hyle_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof =
        new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

    let register_c2 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c2.hyle".into());
    // This technically doesn't matter as it's actually the proof that does the work
    let register_sub_c2 = make_register_tx(
        "toto@c2.hyle".into(),
        "c2.hyle".into(),
        "sub.c2.hyle".into(),
    );

    let mut output = make_hyle_output(register_sub_c2.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContract(RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: "sub.c2.hyle".into(),
            timeout_window: None,
        }));
    let sub_c2_proof = new_proof_tx(&"c2.hyle".into(), &output, &register_sub_c2.hashed());

    state.craft_block_and_handle(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof.into(),
            register_c2.into(),
            register_sub_c2.into(),
            sub_c2_proof.into(),
        ],
    );
    assert_eq!(state.contracts.len(), 4);

    // Now delete the intermediate contract first, then delete the sub-contract via hyle
    let delete_tx = make_delete_tx_with_hyli("hyle".into(), "c2.hyle".into());

    let mut output = make_hyle_output_bis(delete_tx.clone(), BlobIndex(0));
    let verify_hyli_proof = new_proof_tx(&"wallet".into(), &output, &delete_tx.hashed());

    let delete_sub_tx = make_delete_tx_with_hyli("hyle".into(), "sub.c2.hyle".into());
    let mut output = make_hyle_output_ter(delete_sub_tx.clone(), BlobIndex(0));
    let verify_hyli_proof2 = new_proof_tx(&"wallet".into(), &output, &delete_sub_tx.hashed());

    let block = state.craft_block_and_handle(
        2,
        vec![
            delete_tx.into(),
            verify_hyli_proof.into(),
            delete_sub_tx.into(),
            verify_hyli_proof2.into(),
        ],
    );

    assert_eq!(
        block
            .deleted_contracts
            .keys()
            .map(|dce| dce.0.clone())
            .collect::<Vec<_>>(),
        vec!["c2.hyle", "sub.c2.hyle"]
    );
    assert_eq!(state.contracts.len(), 2);
}

#[test_log::test(tokio::test)]
async fn test_register_update_delete_combinations_hyle() {
    let register_tx = make_register_tx("hyle@hyle".into(), "hyle".into(), "c.hyle".into());
    let delete_tx = make_delete_tx_with_hyli("hyle".into(), "c.hyle".into());
    let delete_self_tx = make_delete_tx("hyle@c.hyle".into(), "c.hyle".into(), "c.hyle".into());
    let update_tx = make_register_tx("test@c.hyle".into(), "c.hyle".into(), "c.hyle".into());

    let mut output = make_hyle_output(update_tx.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContract(RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: "c.hyle".into(),
            timeout_window: None,
        }));
    let proof_update = new_proof_tx(&"c.hyle".into(), &output, &update_tx.hashed());

    let mut output =
        make_hyle_output_with_state(delete_self_tx.clone(), BlobIndex(0), &[4, 5, 6], &[1]);
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("c.hyle".into()));
    let proof_delete = new_proof_tx(&"c.hyle".into(), &output, &delete_self_tx.hashed());

    let mut output = make_hyle_output_bis(delete_tx.clone(), BlobIndex(0));
    let delete_tx_proof = new_proof_tx(&"wallet".into(), &output, &delete_tx.hashed());

    async fn test_combination(
        proofs: Option<&[&VerifiedProofTransaction]>,
        txs: &[&BlobTransaction],
        expected_ct: usize,
        expected_txs: usize,
    ) {
        let mut state = new_node_state().await;

        let register_wallet = make_register_tx("hyle@hyle".into(), "hyle".into(), "wallet".into());
        let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

        let mut output = make_hyle_output(register_hyli_at_wallet.clone(), BlobIndex(0));
        let register_hyli_at_wallet_proof =
            new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

        state.craft_block_and_handle(
            1,
            vec![
                register_wallet.clone().into(),
                register_hyli_at_wallet.clone().into(),
                register_hyli_at_wallet_proof.clone().into(),
            ],
        );
        let mut txs = txs
            .iter()
            .map(|tx| (*tx).clone().into())
            .collect::<Vec<_>>();
        if let Some(proofs) = proofs {
            txs.extend(proofs.iter().map(|p| (*p).clone().into()));
        }
        let block = state.craft_block_and_handle(2, txs);

        assert_eq!(state.contracts.len(), expected_ct);
        assert_eq!(block.successful_txs.len(), expected_txs);
        info!("done");
    }

    // Test all combinations
    test_combination(None, &[&register_tx], 3, 1).await;
    test_combination(None, &[&delete_tx], 2, 0).await;
    test_combination(Some(&[&delete_tx_proof]), &[&register_tx, &delete_tx], 2, 2).await;
    test_combination(Some(&[&proof_update]), &[&register_tx, &update_tx], 3, 2).await;
    test_combination(
        Some(&[&proof_update, &delete_tx_proof]),
        &[&register_tx, &update_tx, &delete_tx],
        2,
        3,
    )
    .await;
    test_combination(
        Some(&[&proof_update, &proof_delete]),
        &[&register_tx, &update_tx, &delete_self_tx],
        2,
        3,
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_unknown_contract_and_delete_cleanup() {
    let mut state = new_node_state().await;

    // 1. Create a blob transaction for an unknown contract
    let unknown_contract_tx = BlobTransaction::new(
        "test@unknown",
        vec![Blob {
            contract_name: "unknown".into(),
            data: BlobData(vec![1, 2, 3, 4]),
        }],
    );

    // This transaction should be rejected immediately since the contract doesn't exist
    let block = state.craft_block_and_handle(1, vec![unknown_contract_tx.clone().into()]);
    assert_eq!(block.failed_txs, vec![unknown_contract_tx.hashed()]);

    // 2. Register a contract
    let register_tx = make_register_tx("hyle@hyle".into(), "hyle".into(), "to_delete".into());
    state.craft_block_and_handle(2, vec![register_tx.clone().into()]);

    // 3. Submit blob transactions for the contract but don't settle them
    // The first will delete it, the others are just there to time out.
    let blob_tx1 = make_delete_tx(
        "hyle@to_delete".into(),
        "to_delete".into(),
        "to_delete".into(),
    );
    let blob_tx2 = BlobTransaction::new(
        "test@to_delete",
        vec![Blob {
            contract_name: "to_delete".into(),
            data: BlobData(vec![1, 2, 3, 4]),
        }],
    );
    let blob_tx3 = BlobTransaction::new(
        "test2@to_delete",
        vec![Blob {
            contract_name: "to_delete".into(),
            data: BlobData(vec![5, 6, 7, 8]),
        }],
    );
    let blob_tx1_hash = blob_tx1.hashed();
    let blob_tx2_hash = blob_tx2.hashed();
    let blob_tx3_hash = blob_tx3.hashed();

    state.craft_block_and_handle(
        3,
        vec![blob_tx1.clone().into(), blob_tx2.into(), blob_tx3.into()],
    );

    // Verify transactions are in the unsettled map
    assert!(state.unsettled_transactions.get(&blob_tx1_hash).is_some());
    assert!(state.unsettled_transactions.get(&blob_tx2_hash).is_some());
    assert!(state.unsettled_transactions.get(&blob_tx3_hash).is_some());

    // 4. Delete the contract
    // Create proof for the deletion
    let mut output = make_hyle_output(blob_tx1.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("to_delete".into()));
    let proof_tx = new_proof_tx(&"hyle".into(), &output, &blob_tx1_hash);
    // Execute the deletion
    state.craft_block_and_handle(4, vec![proof_tx.into()]);

    // Verify contract was deleted
    assert!(!state.contracts.contains_key(&"to_delete".into()));

    // Verify all associated transactions were removed from the unsettled map
    assert!(state.unsettled_transactions.get(&blob_tx1_hash).is_none());
    assert!(state.unsettled_transactions.get(&blob_tx2_hash).is_none());
    assert!(state.unsettled_transactions.get(&blob_tx3_hash).is_none());
    assert!(state
        .unsettled_transactions
        .get_next_unsettled_tx(&"to_delete".into())
        .is_none());
}

#[test_log::test(tokio::test)]
async fn test_custom_timeout_then_upgrade_with_none() {
    let mut state = new_node_state().await;

    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_tx(c1.clone());

    // Register the contract
    state.craft_block_and_handle(1, vec![register_c1.into()]);

    let custom_timeout = BlockHeight(150);

    // Upgrade the contract with a custom timeout
    {
        let action = RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: c1.clone(),
            timeout_window: Some(TimeoutWindow::Timeout(custom_timeout)),
            ..Default::default()
        };
        let upgrade_with_timeout = BlobTransaction::new(
            Identity::new("test@c1"),
            vec![action.clone().as_blob("c1".into(), None, None)],
        );

        let upgrade_with_timeout_hash = upgrade_with_timeout.hashed();
        state.craft_block_and_handle(2, vec![upgrade_with_timeout.clone().into()]);

        // Verify the timeout is set correctly - this is the old timeout
        assert_eq!(
            timeouts::tests::get(&state.timeouts, &upgrade_with_timeout_hash),
            Some(BlockHeight(2) + BlockHeight(100))
        );

        // Settle it
        let mut hyle_output = make_hyle_output(upgrade_with_timeout, BlobIndex(0));
        hyle_output
            .onchain_effects
            .push(OnchainEffect::RegisterContract(action.into()));
        let upgrade_with_timeout_proof =
            new_proof_tx(&c1, &hyle_output, &upgrade_with_timeout_hash);
        state.craft_block_and_handle(3, vec![upgrade_with_timeout_proof.into()]);
    }

    // Upgrade the contract again with a None timeout
    {
        let action = RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![4, 5, 6]),
            contract_name: c1.clone(),
            timeout_window: None,
            ..Default::default()
        };
        let upgrade_with_none = BlobTransaction::new(
            Identity::new("test@c1"),
            vec![action.clone().as_blob("c1".into(), None, None)],
        );

        let upgrade_with_none_hash = upgrade_with_none.hashed();
        state.craft_block_and_handle(4, vec![upgrade_with_none.clone().into()]);

        // Verify the timeout is the custom timeout
        assert_eq!(
            timeouts::tests::get(&state.timeouts, &upgrade_with_none_hash),
            Some(BlockHeight(4) + custom_timeout)
        );
        // Settle it
        let mut hyle_output =
            make_hyle_output_with_state(upgrade_with_none, BlobIndex(0), &[4, 5, 6], &[4, 5, 6]);
        hyle_output
            .onchain_effects
            .push(OnchainEffect::RegisterContract(action.into()));
        let upgrade_with_none_proof = new_proof_tx(&c1, &hyle_output, &upgrade_with_none_hash);
        state.craft_block_and_handle(5, vec![upgrade_with_none_proof.into()]);
    }

    // Upgrade the contract again with another custom timeout
    let another_custom_timeout = BlockHeight(200);
    {
        let action = RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![4, 5, 6]),
            contract_name: c1.clone(),
            timeout_window: Some(TimeoutWindow::Timeout(another_custom_timeout)),
            ..Default::default()
        };
        let upgrade_with_another_timeout = BlobTransaction::new(
            Identity::new("test@c1"),
            vec![action.clone().as_blob("c1".into(), None, None)],
        );

        let upgrade_with_another_timeout_hash = upgrade_with_another_timeout.hashed();
        state.craft_block_and_handle(6, vec![upgrade_with_another_timeout.clone().into()]);

        // Verify the timeout is still the OG custom timeout
        assert_eq!(
            timeouts::tests::get(&state.timeouts, &upgrade_with_another_timeout_hash),
            Some(BlockHeight(6) + custom_timeout)
        );
        // Settle it
        let mut hyle_output = make_hyle_output_with_state(
            upgrade_with_another_timeout,
            BlobIndex(0),
            &[4, 5, 6],
            &[4, 5, 6],
        );
        hyle_output
            .onchain_effects
            .push(OnchainEffect::RegisterContract(action.into()));
        let upgrade_with_another_timeout_proof =
            new_proof_tx(&c1, &hyle_output, &upgrade_with_another_timeout_hash);
        state.craft_block_and_handle(7, vec![upgrade_with_another_timeout_proof.into()]);
    }

    // Send a final transaction with no timeout and Check it uses the new timeout
    let final_tx = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![Blob {
            contract_name: c1.clone(),
            data: BlobData(vec![0, 1, 2, 3]),
        }],
    );

    let final_tx_hash = final_tx.hashed();
    state.craft_block_and_handle(8, vec![final_tx.into()]);

    // Verify the timeout remains the same as the last custom timeout
    assert_eq!(
        timeouts::tests::get(&state.timeouts, &final_tx_hash),
        Some(BlockHeight(8) + another_custom_timeout)
    );
}

#[test_log::test(tokio::test)]
async fn test_pending_tx_then_contract_upgrade_and_settlement_order() {
    // 1. Register contract via hyle
    let mut state = new_node_state().await;
    let contract_name = ContractName::new("foo");
    let register_1 = make_register_tx("hyle@hyle".into(), "hyle".into(), contract_name.clone());
    state.craft_block_and_handle(1, vec![register_1.clone().into()]);
    assert!(state.contracts.contains_key(&contract_name));

    // 2. Send a pending TX to that contract (TX2)
    let tx2 = BlobTransaction::new(
        Identity::new("user@foo"),
        vec![Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![1, 2, 3]),
        }],
    );
    let tx2_hash = tx2.hashed();
    state.craft_block_and_handle(2, vec![tx2.clone().into()]);
    assert!(state.unsettled_transactions.get(&tx2_hash).is_some());

    // 3. Register the same contract again via hyle (TX3)
    let register_2 = make_register_tx("hyle@hyle".into(), "hyle".into(), contract_name.clone());
    let register_2_hash = register_2.hashed();
    state.craft_block_and_handle(3, vec![register_2.clone().into()]);

    // 4. Send another TX to the contract (TX4)
    let tx4 = BlobTransaction::new(
        Identity::new("user2@foo"),
        vec![Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![4, 5, 6]),
        }],
    );
    let tx4_hash = tx4.hashed();
    state.craft_block_and_handle(4, vec![tx4.clone().into()]);
    assert!(state.unsettled_transactions.get(&tx4_hash).is_some());

    // 5. Submit the proof for TX4
    let mut output4 = make_hyle_output_with_state(tx4.clone(), BlobIndex(0), &[4, 5, 6], &[1]);
    let proof4 = new_proof_tx(&contract_name, &output4, &tx4_hash);
    let block5 = state.craft_block_and_handle(5, vec![proof4.clone().into()]);
    // TX4 should not be settled yet, because TX2 is still pending
    assert!(state.unsettled_transactions.get(&tx4_hash).is_some());

    // 6. Submit the proof for TX2
    let mut output2 = make_hyle_output(tx2.clone(), BlobIndex(0));
    let proof2 = new_proof_tx(&contract_name, &output2, &tx2_hash);
    let block6 = state.craft_block_and_handle(6, vec![proof2.clone().into()]);

    // 7. Now TX3 (the duplicate register) should have failed, and TX4 should be settled immediately
    // Check block5: TX3 should be in failed_txs
    assert!(block6.failed_txs.contains(&register_2_hash));
    // Check block6: TX4 should be settled (successful_txs)
    assert!(block6.successful_txs.contains(&tx4_hash));
    // TX2 should also be settled
    assert!(block6.successful_txs.contains(&tx2_hash));
    // Both TX2 and TX4 should be removed from unsettled
    assert!(state.unsettled_transactions.get(&tx2_hash).is_none());
    assert!(state.unsettled_transactions.get(&tx4_hash).is_none());
}

#[test_log::test(tokio::test)]
async fn domino_settlement_after_contract_delete() {
    // Register contracts 'a' and 'b' via crafted blocks
    let mut state = new_node_state().await;
    state.contracts.insert(
        ContractName::new("wallet"),
        Contract {
            name: ContractName::new("wallet"),
            program_id: ProgramId(vec![]),
            state: StateCommitment(vec![0, 1, 2, 3]),
            verifier: Verifier("test".into()),
            timeout_window: TimeoutWindow::Timeout(BlockHeight(100)),
        },
    );
    let a = ContractName::new("a");
    let b = ContractName::new("b");
    let register_a = make_register_contract_tx(a.clone());
    let register_b = make_register_contract_tx(b.clone());
    state.craft_block_and_handle(
        1,
        vec![register_a.clone().into(), register_b.clone().into()],
    );

    // Send a transaction to B
    let identity_b = Identity::new("user@b");
    let tx_b = BlobTransaction::new(identity_b.clone(), vec![new_blob("b")]);
    let tx_b_id = tx_b.hashed();
    state.craft_block_and_handle(2, vec![tx_b.clone().into()]);

    // Delete B via hyle (as a tx)
    let delete_b = make_delete_tx_with_hyli(ContractName::new("hyle"), b.clone());
    // And proof for 'wallet'
    let hyle_output_wallet = make_hyle_output(delete_b.clone(), BlobIndex(0));
    let proof_wallet = new_proof_tx(
        &ContractName::new("wallet"),
        &hyle_output_wallet,
        &delete_b.hashed(),
    );
    state.craft_block_and_handle(3, vec![delete_b.clone().into(), proof_wallet.into()]);

    // Send a transaction with blobs for both B and A
    let identity_ab = Identity::new("user@a");
    let tx_ab = BlobTransaction::new(identity_ab.clone(), vec![new_blob("b"), new_blob("a")]);
    let tx_ab_id = tx_ab.hashed();
    state.craft_block_and_handle(4, vec![tx_ab.clone().into()]);

    // Send another transaction for just A
    let identity_a = Identity::new("user@a");
    let tx_a = BlobTransaction::new(identity_a.clone(), vec![new_blob("a")]);
    let tx_a_id = tx_a.hashed();
    state.craft_block_and_handle(5, vec![tx_a.clone().into()]);

    // Send proof to make it ready to settle
    let hyle_output_a = make_hyle_output(tx_a.clone(), BlobIndex(0));
    let proof_a = new_proof_tx(&a, &hyle_output_a, &tx_a_id);
    let block_a = state.craft_block_and_handle(6, vec![proof_a.into()]);

    // Now send the proof for the first transaction (to B)
    let hyle_output_b = make_hyle_output(tx_b.clone(), BlobIndex(0));
    let proof_b = new_proof_tx(&b, &hyle_output_b, &tx_b_id);
    let block_b = state.craft_block_and_handle(7, vec![proof_b.into()]);
    // This should domino through and settle the tx_ab and tx_a if not already settled
    assert!(block_b.successful_txs.contains(&tx_a_id));
}
