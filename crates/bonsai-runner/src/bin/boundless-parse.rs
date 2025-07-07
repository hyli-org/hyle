use std::str::FromStr;

use anyhow::Context;
use boundless_market::{
    alloy::{
        primitives::{utils::parse_ether, Bytes},
        signers::local::PrivateKeySigner,
        transports::http::reqwest::Url,
    },
    client::ClientBuilder,
    deployments::NamedChain,
    storage::storage_provider_from_env,
    Deployment, StorageProvider,
};
use risc0_ethereum_contracts::receipt::{decode_seal, decode_seal_with_claim};
use risc0_zkvm::{
    compute_image_id, sha::Digestible, Groth16Receipt, InnerReceipt, Journal, MaybePruned, Receipt,
    ReceiptClaim, SuccinctReceipt,
};
use tracing::{info, Level};

#[tokio::main]
async fn main() {
    // Initialisation de tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    // let journal: Journal = Journal::new(journal.to_vec());

    // let receipt_claim = ReceiptClaim::ok(image_id, MaybePruned::Pruned(journal.digest()));

    // let claim = ReceiptClaim::ok(image_id, journal.to_vec());
    // let receipt = boundless_client
    //     .set_verifier
    //     .fetch_receipt_with_claim(seal, claim, journal.to_vec())
    //     .await?;

    // let receipt = Receipt::new(
    //     InnerReceipt::Groth16(Groth16Receipt::new(
    //         seal,
    //         MaybePruned::Pruned(journal.digest()),
    //         image_id,
    //     )),
    //     journal.bytes,
    // );
    //
    toto().await.unwrap();
}

async fn toto() -> anyhow::Result<()> {
    // read journal.bin & seal.bin
    let journal_path = "journal.bin";
    let seal_path = "seal.bin";

    let journal = match std::fs::read(journal_path) {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to read journal: {}", e);
            return Ok(());
        }
    };
    let journal: Bytes =
        bincode::deserialize(journal.as_slice()).expect("Failed to deserialize journal");

    let seal = match std::fs::read(seal_path) {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to read seal: {}", e);
            return Ok(());
        }
    };

    let seal: Bytes = Bytes::from(seal);

    let elf = include_bytes!("../../../../../wallet/contracts/wallet/wallet.img");
    let image_id = compute_image_id(elf).unwrap();

    let receipt = decode_seal(seal, image_id, journal)?;

    println!("Receipt: {:?}", receipt);

    receipt
        .set_inclusion_receipt()
        .unwrap()
        .root
        .as_ref()
        .unwrap()
        .verify(image_id)?;

    Ok(())
}
