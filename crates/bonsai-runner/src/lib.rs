use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use bonsai_sdk::non_blocking::Client;
use borsh::BorshSerialize;
use boundless_market::{
    alloy::{
        primitives::{utils::parse_ether, Bytes, Uint},
        signers::local::PrivateKeySigner,
        transports::http::reqwest::Url,
    },
    client::ClientBuilder,
    contracts::Offer,
    deployments::NamedChain,
    storage::{storage_provider_from_env, StorageProvider},
    Deployment, GuestEnvBuilder,
};
use risc0_zkvm::{
    compute_image_id, default_executor, sha::Digestible, Digest, Receipt, ReceiptClaim,
};
use tracing::info;

#[allow(dead_code)]
pub fn as_input_data<T: BorshSerialize>(data: &T) -> Result<Vec<u8>> {
    let data = borsh::to_vec(&data)?;
    let size = risc0_zkvm::serde::to_vec(&data.len())?;
    let mut input_data = bytemuck::cast_slice(&size).to_vec();
    input_data.extend(data);
    Ok(input_data)
}

pub async fn run_boundless(elf: &[u8], input_data: Vec<u8>) -> Result<Receipt> {
    let chain_id = std::env::var("BOUNDLESS_CHAIN_ID").unwrap_or("11155111".to_string());
    let offchain = std::env::var("BOUNDLESS_OFFCHAIN").unwrap_or_default() == "true";
    let wallet_private_key = std::env::var("BOUNDLESS_WALLET_PRIVATE_KEY").unwrap_or_default();
    let rpc_url = std::env::var("BOUNDLESS_RPC_URL").unwrap_or_default();

    let min_price_per_mcycle =
        std::env::var("BOUNDLESS_MIN_PRICE_PER_MCYCLE").unwrap_or_else(|_| "0.000001".to_string());
    let max_price_per_mcycle =
        std::env::var("BOUNDLESS_MAX_PRICE_PER_MCYCLE").unwrap_or_else(|_| "0.000005".to_string());
    let timeout = std::env::var("BOUNDLESS_TIMEOUT").unwrap_or_else(|_| "120".to_string());
    let lock_timeout = std::env::var("BOUNDLESS_LOCK_TIMEOUT").unwrap_or_else(|_| "60".to_string());
    let ramp_up_period =
        std::env::var("BOUNDLESS_RAMP_UP_PERIOD").unwrap_or_else(|_| "15".to_string());

    let chain_id: u64 = chain_id.parse()?;
    let min_price_per_mcycle = parse_ether(&min_price_per_mcycle)?;
    let max_price_per_mcycle = parse_ether(&max_price_per_mcycle)?;
    let timeout: u32 = timeout.parse()?;
    let lock_timeout: u32 = lock_timeout.parse()?;
    let ramp_up_period: u32 = ramp_up_period.parse()?;

    // Creates a storage provider based on the environment variables.
    //
    // If the environment variable `RISC0_DEV_MODE` is set, a temporary file storage provider is used.
    // Otherwise, the following environment variables are checked in order:
    // - `PINATA_JWT`, `PINATA_API_URL`, `IPFS_GATEWAY_URL`: Pinata storage provider;
    // - `S3_ACCESS`, `S3_SECRET`, `S3_BUCKET`, `S3_URL`, `AWS_REGION`: S3 storage provider.
    // TODO: gcp storage provider
    let storage_provider = storage_provider_from_env()?;

    let image_url = storage_provider.upload_program(elf).await?;
    info!("Uploaded image to {}", image_url);

    let wallet_private_key = PrivateKeySigner::from_str(&wallet_private_key)?;
    let rpc_url = Url::parse(&rpc_url)?;

    let mut deployment = Deployment::from_chain_id(chain_id);

    if let Some(dep) = deployment.as_mut() {
        if dep.chain_id.unwrap() == NamedChain::Base as u64 && chain_id == 84532 {
            dep.chain_id = Some(NamedChain::BaseSepolia as u64);
        }
    }

    // Create a Boundless client from the provided parameters.
    let boundless_client = ClientBuilder::new()
        .with_rpc_url(rpc_url)
        .with_deployment(deployment)
        .with_storage_provider(Some(storage_provider))
        .with_private_key(wallet_private_key)
        .build()
        .await
        .context("failed to build boundless client")?;

    // Encode the input and upload it to the storage provider.
    let guest_env = risc0_zkvm::ExecutorEnv::builder()
        .write_slice(&input_data)
        .build()
        .unwrap();

    // Dry run the ELF with the input to get the journal and cycle count.
    // This can be useful to estimate the cost of the proving request.
    // It can also be useful to ensure the guest can be executed correctly and we do not send into
    // the market unprovable proving requests. If you have a different mechanism to get the expected
    // journal and set a price, you can skip this step.
    let session_info = default_executor().execute(guest_env, elf)?;
    let mcycles_count = session_info
        .segments
        .iter()
        .map(|segment| 1 << segment.po2)
        .sum::<u64>()
        .div_ceil(1_000_000);
    let journal = session_info.journal;

    info!(
        "Dry run completed: {} mcycles, journal digest: {}",
        mcycles_count,
        journal.digest()
    );

    let address = boundless_client.signer.as_ref().unwrap().address();
    let balance = boundless_client
        .boundless_market
        .balance_of(address)
        .await?;
    let max_price = max_price_per_mcycle * Uint::from(mcycles_count);
    info!(address = %address, max_price = %max_price, "Wallet balance: {}", balance);
    if balance < max_price {
        let deposit = std::cmp::max(max_price, parse_ether("0.1")?);
        info!(
            "Wallet balance ({}) is low, depositing {} ETH",
            balance, deposit
        );
        boundless_client.boundless_market.deposit(deposit).await?;
    }

    // Create a proof request with the image, input, requirements and offer.
    // The ELF (i.e. image) is specified by the image URL.
    // The input can be specified by an URL, as in this example, or can be posted on chain by using
    // the `with_inline` method with the input bytes.
    // The requirements are the image ID and the digest of the journal. In this way, the market can
    // verify that the proof is correct by checking both the committed image id and digest of the
    // journal. The offer specifies the price range and the timeout for the request.
    // Additionally, the offer can also specify:
    // - the bidding start time: the block number when the bidding starts;
    // - the ramp up period: the number of blocks before the price start increasing until reaches
    //   the maxPrice, starting from the the bidding start;
    // - the lockin price: the price at which the request can be locked in by a prover, if the
    //   request is not fulfilled before the timeout, the prover can be slashed.
    // If the input exceeds 2 kB, upload the input and provide its URL instead, as a rule of thumb.
    // let request_input = if guest_env_bytes.len() > 2 << 10 {
    //     let input_url = boundless_client.upload_input(&guest_env_bytes).await?;
    //     tracing::info!("Uploaded input to {}", input_url);
    //     Input::url(input_url)
    // } else {
    //     tracing::info!("Sending input inline with request");
    //     Input::inline(guest_env_bytes.clone())
    // };
    //
    let env = GuestEnvBuilder::new().write_slice(&input_data).build_env();

    let request = boundless_client
        .new_request()
        .with_program_url(image_url)?
        .with_env(env)
        // .with_requirements(Requirements::new(
        //     compute_image_id(elf)?,
        //     Predicate::digest_match(journal.digest()),
        // ))
        .with_offer(
            Offer::default()
                // The market uses a reverse Dutch auction mechanism to match requests with provers.
                // Each request has a price range that a prover can bid on. One way to set the price
                // is to choose a desired (min and max) price per million cycles and multiply it
                // by the number of cycles. Alternatively, you can use the `with_min_price` and
                // `with_max_price` methods to set the price directly.
                .with_min_price_per_mcycle(min_price_per_mcycle, mcycles_count)
                // NOTE: If your offer is not being accepted, try increasing the max price.
                .with_max_price_per_mcycle(max_price_per_mcycle, mcycles_count)
                // The timeout is the maximum number of blocks the request can stay
                // unfulfilled in the market before it expires. If a prover locks in
                // the request and does not fulfill it before the timeout, the prover can be
                // slashed.
                .with_timeout(timeout)
                .with_lock_timeout(lock_timeout)
                .with_ramp_up_period(ramp_up_period)
                .with_bidding_start(get_current_timestamp_secs()),
        );

    // Send the request and wait for it to be completed.
    let (request_id, expires_at) = if offchain {
        boundless_client.submit_offchain(request).await?
    } else {
        boundless_client.submit_onchain(request).await?
    };
    tracing::info!("Request 0x{request_id:x} submitted");
    tracing::info!("https://explorer.beboundless.xyz/orders/0x{request_id:x}");

    // Wait for the request to be fulfilled by the market, returning the journal and seal.
    let (journal, seal) = boundless_client
        .wait_for_request_fulfillment(request_id, Duration::from_secs(3), expires_at)
        .await?;
    tracing::info!("Request 0x{request_id:x} fulfilled");

    // write journal & seal to disk for debugging purposes
    std::fs::write("journal.bin", bincode::serialize(&journal)?)?;
    std::fs::write("seal.bin", seal.clone())?;

    let image_id = compute_image_id(elf)?;
    let receipt = boundless_client
        .set_verifier
        .fetch_receipt(seal, image_id, journal.to_vec())
        .await?;

    let receipt = receipt.root.ok_or(anyhow::anyhow!(
        "Failed to get root from receipt, this is likely a bug in the SDK"
    ))?;

    receipt.verify(image_id).context("Verify proof")?;

    info!("Receipt verified successfully");

    Ok(receipt)
}

#[allow(dead_code)]
pub async fn run_bonsai(elf: &[u8], input_data: Vec<u8>) -> Result<Receipt> {
    let client = Client::from_env(risc0_zkvm::VERSION)?;

    // Compute the image_id, then upload the ELF with the image_id as its key.
    let image_id = hex::encode(compute_image_id(elf)?);
    client.upload_img(&image_id, elf.to_vec()).await?;

    // Prepare input data and upload it.
    let input_id = client.upload_input(input_data).await?;

    // Add a list of assumptions
    let assumptions: Vec<String> = vec![];

    // Wether to run in execute only mode
    let execute_only = false;

    // Start a session running the prover
    let session = client
        .create_session(image_id, input_id, assumptions, execute_only)
        .await?;
    loop {
        let res = session.status(&client).await?;
        if res.status == "RUNNING" {
            info!(
                "Current status: {} - state: {} - continue polling...",
                res.status,
                res.state.unwrap_or_default()
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        if res.status == "SUCCEEDED" {
            // Download the receipt, containing the output
            let receipt_url = res
                .receipt_url
                .expect("API error, missing receipt on completed session");

            let receipt_buf = client.download(&receipt_url).await?;
            let receipt: Receipt = bincode::deserialize(&receipt_buf)?;
            return Ok(receipt);
        } else {
            bail!(
                "Workflow exited: {} - | err: {}",
                res.status,
                res.error_msg.unwrap_or_default()
            );
        }
    }
}

pub fn get_current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}
