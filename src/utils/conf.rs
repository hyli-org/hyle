use anyhow::{Context, Result};
use config::{Config, Environment, File};
use hyle_modules::modules::websocket::WebSocketConfig;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DurationMilliSeconds;
use std::{collections::HashMap, fmt::Debug, path::PathBuf, sync::Arc, time::Duration};
use strum_macros::IntoStaticStr;

use crate::indexer::IndexerConf;

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Consensus {
    #[serde_as(as = "DurationMilliSeconds")]
    pub slot_duration: Duration,
    #[serde_as(as = "DurationMilliSeconds")]
    pub timeout_after: Duration,
    /// Checks during consensus that blocks have legit timestamps
    pub timestamp_checks: TimestampCheck,
    /// Whether the network runs as a single node or with a multi-node consensus.
    pub solo: bool,
    /// The timestamp of the genesis block, in seconds since the Unix epoch.
    pub genesis_timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, IntoStaticStr)]
pub enum TimestampCheck {
    /// Checks that timestamps are not in the future, and not too old.
    #[default]
    Full,
    /// Checks that timestamps are growing
    Monotonic,
    /// Does not check timestamps
    NoCheck,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GenesisConf {
    /// Initial bonded stakers and their stakes
    pub stakers: HashMap<String, u64>,
    /// Used for testing - if true, token balance will remain in the faucet.
    pub keep_tokens_in_faucet: bool,
}

/// Configuration for the P2P layer
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct P2pConf {
    pub mode: P2pMode,
    /// Public IP address of the p2p server
    pub public_address: String,
    /// Port to listen for incoming connections
    pub server_port: u16,
    /// Max frame length
    pub max_frame_length: usize,
    /// IPs of peers to connect to
    pub peers: Vec<String>,
    /// Time in milliseconds between pings to peers
    pub ping_interval: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
pub enum P2pMode {
    /// Run a full node with a validator participating in consensus
    FullValidator,
    /// Run a full node without consensus (assumes you have your own lane)
    LaneManager,
    /// Run a limited node that subscribes to another one for DA
    #[default]
    None,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct NodeWebSocketConfig {
    /// Wether the WebSocket server is enabled
    pub enabled: bool,
    /// The port number to bind the WebSocket server to
    pub server_port: u16,
    /// The endpoint path for WebSocket connections
    pub ws_path: String,
    /// The endpoint path for health checks
    pub health_path: String,
    /// The interval at which to check for new peers
    pub peer_check_interval: u64,
    /// List of events to stream on the websocket
    pub events: Vec<String>,
}

impl From<NodeWebSocketConfig> for WebSocketConfig {
    fn from(config: NodeWebSocketConfig) -> Self {
        Self {
            port: config.server_port,
            ws_path: config.ws_path,
            health_path: config.health_path,
            peer_check_interval: Duration::from_millis(config.peer_check_interval),
        }
    }
}

pub type SharedConf = Arc<Conf>;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Conf {
    /// Human-readable identifier for this node.
    pub id: String,

    /// The log format to use - "json", "node" or "full" (default)
    pub log_format: String,
    /// Directory name to store node state.
    pub data_directory: PathBuf,

    /// Peer-to-peer layer configuration
    pub p2p: P2pConf,

    // Validator options
    /// Consensus configuration
    pub consensus: Consensus,
    /// Genesis block configuration
    pub genesis: GenesisConf,

    // Module options below
    /// Public IP address of the DA port of the node.
    pub da_public_address: String,
    /// Server port for the DA API
    pub da_server_port: u16,
    /// Server port for the DA API
    pub da_max_frame_length: usize,

    pub run_rest_server: bool,
    /// Server port for the REST API
    pub rest_server_port: u16,
    /// Maximum body size for REST requests
    pub rest_server_max_body_size: usize,

    pub run_admin_server: bool,
    /// Server port for the admin API
    pub admin_server_port: u16,
    /// Maximum body size for admin requests
    pub admin_server_max_body_size: usize,

    pub run_tcp_server: bool,
    /// Server port for the TCP API
    pub tcp_server_port: u16,

    /// Whether to run the indexer (write to db)
    pub run_indexer: bool,
    /// Whether to run the explorer (read from db)
    pub run_explorer: bool,

    /// If running the indexer, the postgres address to connect to
    pub database_url: String,
    /// When running only the indexer, the address of the DA server to connect to
    pub da_read_from: String,
    /// Timeout for DA client requests, in seconds, before it tries to reconnect to stream blocks
    pub da_timeout_client_secs: u64,

    /// Websocket configuration
    pub websocket: NodeWebSocketConfig,

    /// Configuration for the indexer module
    pub indexer: IndexerConf,
}

impl Conf {
    pub fn new(
        config_files: Vec<String>,
        data_directory: Option<String>,
        run_indexer: Option<bool>,
    ) -> Result<Self, anyhow::Error> {
        let mut s = Config::builder().add_source(File::from_str(
            include_str!("conf_defaults.toml"),
            config::FileFormat::Toml,
        ));
        // Priority order: config file, then environment variables, then CLI
        for config_file in config_files {
            s = s.add_source(File::with_name(&config_file).required(false));
        }
        let mut conf: Self = s
            .add_source(
                Environment::with_prefix("hyle")
                    .separator("__")
                    .prefix_separator("_")
                    .list_separator(",")
                    .with_list_parse_key("p2p.peers") // Parse this key into Vec<String>
                    .try_parsing(true),
            )
            .set_override_option("data_directory", data_directory)?
            .set_override_option("run_indexer", run_indexer)?
            .set_override_option("run_explorer", run_indexer)?
            .build()?
            .try_deserialize()?;
        // Mostly for convenience, ignore ourself from the peers list
        conf.p2p
            .peers
            .retain(|peer| peer != &conf.p2p.public_address);

        if conf.consensus.solo {
            conf.genesis.stakers.insert(
                conf.id.clone(),
                match std::env::var("HYLE_SINGLE_NODE_STAKE") {
                    Ok(stake) => stake.parse::<u64>().context("Failed to parse stake"),
                    Err(e) => Err(Into::into(e)),
                }
                .unwrap_or(1000),
            );
        }
        Ok(conf)
    }
}

#[cfg(test)]
mod tests {
    use assertables::assert_ok;

    use super::*;

    #[test]
    fn test_load_default_conf() {
        assert_ok!(Conf::new(vec![], None, None));
    }

    #[test]
    fn test_override_da_public_address() {
        let conf = Conf::new(vec![], None, None).unwrap();
        assert_eq!(conf.da_public_address, "127.0.0.1:4141");
        // All single underscores as there is no nesting.
        std::env::set_var("HYLE_DA_PUBLIC_ADDRESS", "127.0.0.1:9090");
        let conf = Conf::new(vec![], None, None).unwrap();
        assert_eq!(conf.da_public_address, "127.0.0.1:9090");
    }
    #[test]
    fn test_override_p2p_public_address() {
        let conf = Conf::new(vec![], None, None).unwrap();
        assert_eq!(conf.p2p.public_address, "127.0.0.1:1231");
        // Note the double underscore
        std::env::set_var("HYLE_P2P__PUBLIC_ADDRESS", "127.0.0.1:9090");
        let conf = Conf::new(vec![], None, None).unwrap();
        assert_eq!(conf.p2p.public_address, "127.0.0.1:9090");
    }
}
