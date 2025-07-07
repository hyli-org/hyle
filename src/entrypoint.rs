#![allow(clippy::expect_used, reason = "Fail on misconfiguration")]

use crate::{
    bus::{metrics::BusMetrics, SharedMessageBus},
    consensus::Consensus,
    data_availability::DataAvailability,
    explorer::Explorer,
    genesis::Genesis,
    indexer::Indexer,
    mempool::Mempool,
    model::{api::NodeInfo, SharedRunContext},
    node_state::module::NodeStateModule,
    p2p::P2P,
    rest::{ApiDoc, RestApi, RestApiRunContext},
    single_node_consensus::SingleNodeConsensus,
    tcp_server::TcpServer,
    utils::{
        conf::{self, P2pMode},
        modules::ModulesHandler,
    },
};
use anyhow::{bail, Context, Result};
use axum::Router;
use hydentity::Hydentity;
use hyle_crypto::SharedBlstCrypto;
use hyle_modules::{
    modules::{
        admin::{AdminApi, AdminApiRunContext},
        bus_ws_connector::{NodeWebsocketConnector, NodeWebsocketConnectorCtx, WebsocketOutEvent},
        contract_state_indexer::{ContractStateIndexer, ContractStateIndexerCtx},
        da_listener::{DAListener, DAListenerConf},
        websocket::WebSocketModule,
        BuildApiContextInner,
    },
    node_state::module::NodeStateCtx,
};
use hyllar::Hyllar;
use prometheus::Registry;
use smt_token::account::AccountSMT;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};
use tracing::{error, info};
use utoipa::OpenApi;

pub struct RunPg {
    data_dir: PathBuf,
    #[allow(dead_code)]
    pg: ContainerAsync<Postgres>,
}

impl RunPg {
    pub async fn new(config: &mut conf::Conf) -> Result<Self> {
        if std::fs::metadata(&config.data_directory).is_ok() {
            bail!(
                "Data directory {} exists. --pg flag is given, please clean data dir first.",
                config.data_directory.display()
            );
        }

        info!("🐘 Starting postgres DB with default settings for the indexer");
        let pg = Postgres::default()
            .with_tag("17-alpine")
            .with_cmd(["postgres", "-c", "log_statement=all"])
            .start()
            .await?;

        std::thread::sleep(std::time::Duration::from_secs(3));

        config.database_url = format!(
            "postgres://postgres:postgres@localhost:{}/postgres",
            pg.get_host_port_ipv4(5432).await?
        );
        config.run_indexer = true;
        config.run_explorer = true;

        Ok(Self {
            pg,
            data_dir: config.data_directory.clone(),
        })
    }
}

impl Drop for RunPg {
    fn drop(&mut self) {
        tracing::warn!("--pg option given. Postgres server will stop. Cleaning data dir");
        if let Err(e) = std::fs::remove_dir_all(&self.data_dir) {
            error!("Error cleaning data dir: {:?}", e);
        }
    }
}

fn mask_postgres_uri(uri: &str) -> String {
    // On cherche le prefix postgres://user:pass@...
    if let Some(start) = uri.find("://") {
        if let Some(at) = uri[start + 3..].find('@') {
            let creds_part = &uri[start + 3..start + 3 + at];
            if let Some(colon) = creds_part.find(':') {
                let user = &creds_part[..colon];
                let rest = &uri[start + 3 + at..]; // tout après @
                return format!("postgres://{}:{}{}", user, "*****", rest);
            }
        }
    }
    uri.to_string() // fallback : renvoyer tel quel si pas reconnu
}

pub fn welcome_message(conf: &conf::Conf) {
    let version = env!("CARGO_PKG_VERSION");

    let check_or_cross = |val: bool| {
        if val {
            "✔"
        } else {
            "✘"
        }
    };

    tracing::info!(
        r#"

                                    
   ██╗  ██╗██╗   ██╗██╗     ██╗     {mode} [{id}] v{version} 
   ██║  ██║╚██╗ ██╔╝██║     ██║         {validator_details}
   ███████║ ╚████╔╝ ██║     ██║       {check_p2p} p2p::{p2p_port} | {check_http} http::{http_port} | {check_tcp} tcp::{tcp_port} | ◆ da::{da_port}
   ██╔══██║  ╚██╔╝  ██║     ██║     
   ██║  ██║   ██║   ███████╗██║     {check_indexer} indexer {check_explorer} explorer db: {database_url}
   ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝     ∎ {data_directory}
 
   Minimal, yet sufficient. Hope You Like It.
                                 
    "#,
        version = version,
        id = conf.id,
        mode = if conf.p2p.mode == P2pMode::FullValidator {
            "⇄  Validator"
        } else if conf.p2p.mode == P2pMode::LaneManager {
            "≡  Lane Operator"
        } else {
            "✘ NO P2P"
        },
        check_p2p = check_or_cross(!matches!(conf.p2p.mode, P2pMode::None)),
        p2p_port = conf.p2p.server_port,
        check_http = check_or_cross(conf.run_rest_server),
        http_port = conf.rest_server_port,
        check_tcp = check_or_cross(conf.run_tcp_server),
        tcp_port = conf.tcp_server_port,
        da_port = conf.da_public_address,
        check_indexer = check_or_cross(conf.run_indexer),
        check_explorer = check_or_cross(conf.run_explorer),
        database_url = if conf.run_indexer || conf.run_explorer {
            format!("↯ {}", mask_postgres_uri(conf.database_url.as_str()))
        } else {
            "".to_string()
        },
        data_directory = conf.data_directory.to_string_lossy(),
        validator_details = if matches!(conf.p2p.mode, P2pMode::FullValidator) {
            let timestamp_checks: &'static str = (&conf.consensus.timestamp_checks).into();
            let c_mode = if conf.consensus.solo {
                "single"
            } else {
                "multi"
            };
            let sd = conf.consensus.slot_duration.as_millis();
            let peers = if conf.consensus.solo {
                "".to_string()
            } else {
                format!("| peers: [{}]", conf.p2p.peers.join(" ")).to_string()
            };
            format!("{c_mode} | {sd}ms | timestamps: {timestamp_checks} {peers}")
        } else {
            "".to_string()
        },
    );
}

pub async fn main_loop(config: conf::Conf, crypto: Option<SharedBlstCrypto>) -> Result<()> {
    let mut handler = common_main(config, crypto).await?;
    handler.exit_loop().await?;

    Ok(())
}

pub async fn main_process(config: conf::Conf, crypto: Option<SharedBlstCrypto>) -> Result<()> {
    let mut handler = common_main(config, crypto).await?;
    handler.exit_process().await?;

    Ok(())
}

async fn common_main(
    config: conf::Conf,
    crypto: Option<SharedBlstCrypto>,
) -> Result<ModulesHandler> {
    let config = Arc::new(config);

    welcome_message(&config);
    info!("Starting node with config: {:?}", &config);

    let registry = Registry::new();
    // Init global metrics meter we expose as an endpoint
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(
            opentelemetry_prometheus::exporter()
                .with_registry(registry.clone())
                .build()
                .context("starting prometheus exporter")?,
        )
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());

    #[cfg(feature = "monitoring")]
    {
        let scope = opentelemetry::InstrumentationScope::builder(config.id.clone()).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);
        let alloc_metric = my_meter.u64_gauge("malloc_allocated_size").build();
        let alloc_metric2 = my_meter.u64_gauge("malloc_allocations").build();
        let latency_metric = my_meter.u64_histogram("tokio_latency").build();
        // Measure the event loop latency
        // Bit of a noisey hack, but it's indicative.
        tokio::spawn(async move {
            let mut latency = tokio::time::Instant::now();
            loop {
                latency_metric.record(latency.elapsed().as_millis() as u64 - 250, &[]);
                latency = tokio::time::Instant::now();
                tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
            }
        });
        tokio::spawn(async move {
            loop {
                let metrics = alloc_metrics::global_metrics();
                alloc_metric.record(metrics.allocated_bytes as u64, &[]);
                alloc_metric2.record(metrics.allocations as u64, &[]);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });
    }

    let bus = SharedMessageBus::new(BusMetrics::global(config.id.clone()));

    std::fs::create_dir_all(&config.data_directory).context("creating data directory")?;

    let build_api_ctx = Arc::new(BuildApiContextInner {
        router: Mutex::new(Some(Router::new())),
        openapi: Mutex::new(ApiDoc::openapi()),
    });

    let mut handler = ModulesHandler::new(&bus).await;

    if config.run_indexer {
        handler
            .build_module::<Indexer>((config.clone(), build_api_ctx.clone()))
            .await?;
        handler
            .build_module::<ContractStateIndexer<Hyllar>>(ContractStateIndexerCtx {
                contract_name: "hyllar".into(),
                data_directory: config.data_directory.clone(),
                api: build_api_ctx.clone(),
            })
            .await?;
        handler
            .build_module::<ContractStateIndexer<Hyllar>>(ContractStateIndexerCtx {
                contract_name: "hyllar2".into(),
                data_directory: config.data_directory.clone(),
                api: build_api_ctx.clone(),
            })
            .await?;
        handler
            .build_module::<ContractStateIndexer<Hydentity>>(ContractStateIndexerCtx {
                contract_name: "hydentity".into(),
                data_directory: config.data_directory.clone(),
                api: build_api_ctx.clone(),
            })
            .await?;
        handler
            .build_module::<ContractStateIndexer<AccountSMT>>(ContractStateIndexerCtx {
                contract_name: "oranj".into(),
                data_directory: config.data_directory.clone(),
                api: build_api_ctx.clone(),
            })
            .await?;
        handler
            .build_module::<ContractStateIndexer<AccountSMT>>(ContractStateIndexerCtx {
                contract_name: "oxygen".into(),
                data_directory: config.data_directory.clone(),
                api: build_api_ctx.clone(),
            })
            .await?;
        handler
            .build_module::<ContractStateIndexer<AccountSMT>>(ContractStateIndexerCtx {
                contract_name: "vitamin".into(),
                data_directory: config.data_directory.clone(),
                api: build_api_ctx.clone(),
            })
            .await?;
    }

    if config.run_explorer {
        if !config.run_indexer {
            bail!("Cannot run explorer without indexer");
        }

        handler
            .build_module::<Explorer>((config.clone(), build_api_ctx.clone()))
            .await?;
    }

    if config.p2p.mode != conf::P2pMode::None {
        let ctx = SharedRunContext {
            config: config.clone(),
            api: build_api_ctx.clone(),
            crypto: crypto
                .as_ref()
                .expect("Crypto must be defined to run p2p")
                .clone(),
        };

        handler
            .build_module::<NodeStateModule>(NodeStateCtx {
                node_id: config.id.clone(),
                data_directory: config.data_directory.clone(),
                api: build_api_ctx.clone(),
            })
            .await?;

        handler
            .build_module::<DataAvailability>(ctx.clone())
            .await?;

        handler.build_module::<Mempool>(ctx.clone()).await?;

        handler.build_module::<Genesis>(ctx.clone()).await?;

        if config.p2p.mode == conf::P2pMode::FullValidator {
            if config.consensus.solo {
                handler
                    .build_module::<SingleNodeConsensus>(ctx.clone())
                    .await?;
            } else {
                handler.build_module::<Consensus>(ctx.clone()).await?;
            }
        }

        handler.build_module::<P2P>(ctx.clone()).await?;
    } else if config.run_indexer {
        handler
            .build_module::<DAListener>(DAListenerConf {
                data_directory: config.data_directory.clone(),
                da_read_from: config.da_read_from.clone(),
                start_block: None,
                timeout_client_secs: config.da_timeout_client_secs,
            })
            .await?;
    }

    if config.websocket.enabled {
        handler
            .build_module::<WebSocketModule<(), WebsocketOutEvent>>(config.websocket.clone().into())
            .await?;

        handler
            .build_module::<NodeWebsocketConnector>(NodeWebsocketConnectorCtx {
                events: config.websocket.events.clone(),
            })
            .await?;
    }

    if config.run_rest_server {
        // Should come last so the other modules have nested their own routes.
        let router = build_api_ctx
            .router
            .lock()
            .expect("Context router should be available.")
            .take()
            .expect("Context router should be available.");
        let openapi = build_api_ctx
            .openapi
            .lock()
            .expect("OpenAPI should be available")
            .clone();

        handler
            .build_module::<RestApi>(
                RestApiRunContext::new(
                    config.rest_server_port,
                    NodeInfo {
                        id: config.id.clone(),
                        pubkey: crypto.as_ref().map(|c| c.validator_pubkey()).cloned(),
                        da_address: config.da_public_address.clone(),
                    },
                    router.clone(),
                    config.rest_server_max_body_size,
                    openapi,
                )
                .with_registry(registry),
            )
            .await?;
    }

    if config.run_admin_server {
        handler
            .build_module::<AdminApi>(AdminApiRunContext::new(
                config.admin_server_port,
                Router::new(),
                config.admin_server_max_body_size,
                config.data_directory.clone(),
            ))
            .await?;
    }

    if config.run_tcp_server {
        handler
            .build_module::<TcpServer>(config.tcp_server_port)
            .await?;
    }

    _ = handler.start_modules().await;

    Ok(handler)
}
