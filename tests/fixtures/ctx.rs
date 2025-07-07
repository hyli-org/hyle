#![allow(unused)]
#![allow(clippy::indexing_slicing)]

use std::{
    net::{Ipv4Addr, TcpListener},
    time::Duration,
};

use anyhow::{Context, Result};
use api::APIContract;
use assertables::assert_ok;
use client_sdk::{rest_client::NodeApiClient, transaction_builder::ProvableBlobTx};
use hyle_model::api::APINodeContract;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};
use tracing::info;

use hyle::{
    model::*,
    rest::client::{IndexerApiHttpClient, NodeApiHttpClient},
    utils::conf::{Conf, P2pMode, TimestampCheck},
};
use hyle_contract_sdk::{
    BlobIndex, ContractName, HyleOutput, Identity, ProgramId, StateCommitment, TxHash, Verifier,
};
use hyle_net::net::bind_tcp_listener;

use crate::fixtures::test_helpers::{wait_height_timeout, IndexerOrNodeHttpClient};

use super::test_helpers::{self, wait_height, wait_indexer_height, ConfMaker};

pub trait E2EContract {
    fn verifier() -> Verifier;
    fn program_id() -> ProgramId;
    fn state_commitment() -> StateCommitment;
}

pub struct E2ECtx {
    pg: Option<ContainerAsync<Postgres>>,
    nodes: Vec<test_helpers::TestProcess>,
    clients: Vec<NodeApiHttpClient>,
    client_index: usize,
    indexer_client: Option<IndexerApiHttpClient>,
    slot_duration: Duration,
}

impl E2ECtx {
    async fn init() -> ContainerAsync<Postgres> {
        // Start postgres DB with default settings for the indexer.
        Postgres::default()
            .with_tag("17-alpine")
            .with_cmd(["postgres", "-c", "log_statement=all"])
            .start()
            .await
            .unwrap()
    }

    async fn build_nodes(
        count: usize,
        conf_maker: &mut ConfMaker,
    ) -> (Vec<test_helpers::TestProcess>, Vec<NodeApiHttpClient>) {
        let mut nodes = Vec::new();
        let mut clients = Vec::new();
        let mut peers = Vec::new();
        let mut confs = Vec::new();
        let mut genesis_stakers = std::collections::HashMap::new();

        for i in 0..count {
            let mut node_conf = conf_maker.build("node").await;
            node_conf.p2p.peers = peers.clone();
            genesis_stakers.insert(node_conf.id.clone(), 100);
            peers.push(node_conf.p2p.public_address.clone());
            confs.push(node_conf);
        }

        for node_conf in confs.iter_mut() {
            node_conf.genesis.stakers = genesis_stakers.clone();
            let node = test_helpers::TestProcess::new("hyle", node_conf.clone()).start();

            // Request something on node1 to be sure it's alive and working
            let client = NodeApiHttpClient::new(format!(
                "http://localhost:{}/",
                &node.conf.rest_server_port
            ))
            .expect("Creating http client for node");
            nodes.push(node);
            clients.push(client);
        }
        (nodes, clients)
    }

    pub async fn new_single(slot_duration_ms: u64) -> Result<E2ECtx> {
        std::env::set_var("RISC0_DEV_MODE", "1");

        let mut conf_maker = ConfMaker::default();
        conf_maker.default.consensus.slot_duration = Duration::from_millis(slot_duration_ms);
        conf_maker.default.consensus.solo = true;
        conf_maker.default.genesis.stakers =
            vec![("single-node".to_string(), 100)].into_iter().collect();

        let node_conf = conf_maker.build("single-node").await;
        let node = test_helpers::TestProcess::new("hyle", node_conf).start();

        // Request something on node1 to be sure it's alive and working
        let client =
            NodeApiHttpClient::new(format!("http://localhost:{}/", &node.conf.rest_server_port))
                .expect("Creating http client for node");

        while client.get_node_info().await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("🚀 E2E test environment is ready!");
        Ok(E2ECtx {
            pg: None,
            nodes: vec![node],
            clients: vec![client],
            client_index: 0,
            indexer_client: None,
            slot_duration: Duration::from_millis(slot_duration_ms),
        })
    }

    pub async fn new_single_with_indexer(slot_duration_ms: u64) -> Result<E2ECtx> {
        std::env::set_var("RISC0_DEV_MODE", "1");

        let pg = Self::init().await;

        let mut conf_maker = ConfMaker::default();
        conf_maker.default.consensus.slot_duration = Duration::from_millis(slot_duration_ms);
        conf_maker.default.consensus.solo = true;
        conf_maker.default.genesis.stakers =
            vec![("single-node".to_string(), 100)].into_iter().collect();
        conf_maker.default.database_url = format!(
            "postgres://postgres:postgres@localhost:{}/postgres",
            pg.get_host_port_ipv4(5432).await.unwrap()
        );

        let node_conf = conf_maker.build("single-node").await;
        let node = test_helpers::TestProcess::new("hyle", node_conf.clone()).start();

        // Request something on node1 to be sure it's alive and working
        let client =
            NodeApiHttpClient::new(format!("http://localhost:{}/", &node.conf.rest_server_port))
                .expect("Creating http client for node");

        // Start indexer
        let mut indexer_conf = conf_maker.build("indexer").await;
        indexer_conf.da_read_from = node_conf.da_public_address.clone();
        indexer_conf.run_indexer = true;
        indexer_conf.run_explorer = true;
        let indexer = test_helpers::TestProcess::new("indexer", indexer_conf.clone()).start();

        let url = format!("http://localhost:{}/", &indexer_conf.rest_server_port);
        let indexer_client = IndexerApiHttpClient::new(url).unwrap();

        while indexer_client.get_last_block().await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        while client.get_node_info().await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("🚀 E2E test environment is ready!");
        Ok(E2ECtx {
            pg: Some(pg),
            nodes: vec![node, indexer],
            clients: vec![client],
            client_index: 0,
            indexer_client: Some(indexer_client),
            slot_duration: Duration::from_millis(slot_duration_ms),
        })
    }
    pub async fn new_multi(count: usize, slot_duration_ms: u64) -> Result<E2ECtx> {
        std::env::set_var("RISC0_DEV_MODE", "1");

        let mut conf_maker = ConfMaker::default();
        conf_maker.default.consensus.slot_duration = Duration::from_millis(slot_duration_ms);

        let (nodes, clients) = Self::build_nodes(count, &mut conf_maker).await;
        wait_height_timeout(
            &IndexerOrNodeHttpClient::Node(clients.first().unwrap().clone()),
            1,
            120,
        )
        .await?;

        loop {
            let mut stop = true;
            for client in clients.iter() {
                if client.get_node_info().await.is_err() {
                    stop = false
                }
            }
            if stop {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("🚀 E2E test environment is ready!");
        Ok(E2ECtx {
            pg: None,
            nodes,
            clients,
            client_index: 0,
            indexer_client: None,
            slot_duration: Duration::from_millis(slot_duration_ms),
        })
    }

    pub async fn make_conf(&self, prefix: &str) -> Conf {
        let mut conf_maker = ConfMaker::default();
        let mut node_conf = conf_maker.build(prefix).await;
        node_conf.consensus.slot_duration = self.slot_duration;
        node_conf.p2p.peers = self
            .nodes
            .iter()
            .filter_map(|node| {
                if node.conf.p2p.mode != P2pMode::None {
                    Some(node.conf.p2p.public_address.clone())
                } else {
                    None
                }
            })
            .collect();
        node_conf
    }

    pub async fn add_node(&mut self) -> Result<&NodeApiHttpClient> {
        self.add_node_with_conf(self.make_conf("new_node").await)
            .await
    }

    pub async fn add_node_with_conf(&mut self, node_conf: Conf) -> Result<&NodeApiHttpClient> {
        let node = test_helpers::TestProcess::new("hyle", node_conf).start();
        // Request something on node1 to be sure it's alive and working
        let client =
            NodeApiHttpClient::new(format!("http://localhost:{}/", &node.conf.rest_server_port))
                .expect("Creating http client for node");

        wait_height(&client, 1).await?;
        self.nodes.push(node);
        self.clients.push(client);
        Ok(self.clients.last().unwrap())
    }
    pub async fn new_multi_with_indexer(count: usize, slot_duration_ms: u64) -> Result<E2ECtx> {
        Self::new_multi_with_indexer_and_timestamp_checks(
            count,
            slot_duration_ms,
            TimestampCheck::Full,
        )
        .await
    }

    pub async fn new_multi_with_indexer_and_timestamp_checks(
        count: usize,
        slot_duration_ms: u64,
        timestamp_checks: TimestampCheck,
    ) -> Result<E2ECtx> {
        std::env::set_var("RISC0_DEV_MODE", "1");

        let pg = Self::init().await;

        let mut conf_maker = ConfMaker::default();
        conf_maker.default.consensus.slot_duration = Duration::from_millis(slot_duration_ms);
        conf_maker.default.database_url = format!(
            "postgres://postgres:postgres@localhost:{}/postgres",
            pg.get_host_port_ipv4(5432).await.unwrap()
        );
        conf_maker.default.consensus.timestamp_checks = timestamp_checks;

        let (mut nodes, mut clients) = Self::build_nodes(count, &mut conf_maker).await;

        // Start indexer
        let mut indexer_conf = conf_maker.build("indexer").await;
        indexer_conf.run_indexer = true;
        indexer_conf.run_explorer = true;
        indexer_conf.da_read_from = nodes.last().unwrap().conf.da_public_address.clone();
        let indexer = test_helpers::TestProcess::new("indexer", indexer_conf.clone()).start();

        nodes.push(indexer);
        let url = format!("http://localhost:{}/", &indexer_conf.rest_server_port);

        let indexer_client = IndexerApiHttpClient::new(url).unwrap();

        loop {
            let mut stop = true;
            for client in clients.iter() {
                if client.get_node_info().await.is_err() {
                    stop = false
                }
            }
            if stop {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        while indexer_client.get_last_block().await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let pg = Some(pg);

        info!("🚀 E2E test environment is ready!");
        Ok(E2ECtx {
            pg,
            nodes,
            clients,
            client_index: 0,
            indexer_client: Some(indexer_client),
            slot_duration: Duration::from_millis(slot_duration_ms),
        })
    }

    pub async fn stop_all(&mut self) {
        futures::future::join_all(self.nodes.iter_mut().map(|node| node.stop())).await;
    }

    pub async fn stop_node(&mut self, index: usize) -> Result<()> {
        self.nodes[index].stop().await
    }

    pub fn restart_node(&mut self, index: usize) -> Result<()> {
        // Stupid but it works
        let node = self.nodes.remove(index).start();
        self.nodes.insert(index, node);
        Ok(())
    }

    pub fn get_instructions_for(&mut self, index: usize) {
        // Print instructions to start the node manually outside the test context.
        tracing::warn!(
            "🚀 Start node with the following command:\nhyle=$(pwd)/target/release/hyle && (cd {} && RUST_LOG=info \"$hyle\")",
            self.nodes[index].dir.path().display()
        );
    }

    pub fn client(&self) -> &NodeApiHttpClient {
        &self.clients[self.client_index]
    }

    pub fn client_by_id(&self, name: &str) -> &NodeApiHttpClient {
        let mut post_indexer = false;
        let mut i = self
            .nodes
            .iter()
            .inspect(|node| {
                if node.conf.id.contains("indexer") {
                    post_indexer = true;
                }
            })
            .position(|node| node.conf.id == name)
            .unwrap();
        i = if post_indexer { i - 1 } else { i };
        &self.clients[i]
    }

    pub fn indexer_client(&self) -> &IndexerApiHttpClient {
        self.indexer_client.as_ref().unwrap()
    }

    pub fn has_indexer(&self) -> bool {
        self.pg.is_some()
    }

    pub async fn metrics(&self) -> Result<String> {
        let metrics = self.client().metrics().await?;

        Ok(metrics)
    }

    pub async fn register_contract<Contract>(&self, sender: Identity, name: &str) -> Result<()>
    where
        Contract: E2EContract,
    {
        let blobs = vec![RegisterContractAction {
            verifier: Contract::verifier(),
            program_id: Contract::program_id(),
            state_commitment: Contract::state_commitment(),
            contract_name: name.into(),
            ..Default::default()
        }
        .as_blob("hyle".into(), None, None)];

        let tx = BlobTransaction::new(sender.clone(), blobs.clone());
        assert_ok!(self.client().send_tx_blob(tx).await);

        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let resp = self.client().get_contract(name.into()).await;
                if resp.is_err() || resp.is_err() {
                    info!("⏰ Waiting for contract {name} state to be ready");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                } else {
                    return;
                }
            }
        })
        .await?;
        Ok(())
    }

    pub async fn send_blob(&self, identity: Identity, blobs: Vec<Blob>) -> Result<TxHash> {
        self.client()
            .send_tx_blob(BlobTransaction::new(identity, blobs))
            .await
    }

    pub async fn send_provable_blob_tx(&self, tx: &ProvableBlobTx) -> Result<TxHash> {
        self.client()
            .send_tx_blob(BlobTransaction::new(tx.identity.clone(), tx.blobs.clone()))
            .await
    }

    pub async fn send_proof_single(&self, proof: ProofTransaction) -> Result<()> {
        assert_ok!(self.client().send_tx_proof(proof).await);
        Ok(())
    }

    pub async fn send_proof(
        &self,
        contract_name: ContractName,
        proof: ProofData,
        verifies: Vec<TxHash>,
    ) -> Result<()> {
        assert_ok!(
            self.client()
                .send_tx_proof(ProofTransaction {
                    contract_name: contract_name.clone(),
                    proof: proof.clone(),
                })
                .await
        );

        Ok(())
    }

    pub async fn wait_height(&self, height: u64) -> Result<()> {
        wait_height(self.client(), height).await
    }

    pub async fn wait_indexer_height(&self, height: u64) -> Result<()> {
        wait_indexer_height(self.indexer_client(), height).await
    }

    pub async fn get_contract(&self, name: &str) -> Result<APINodeContract> {
        self.client().get_contract(name.into()).await
    }
}
