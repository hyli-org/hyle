//! Index system for historical data.

pub mod api;

use crate::{model::*, utils::conf::SharedConf};
use anyhow::{Context, Result};
use api::*;
use axum::extract::ws::Message;
use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use hyle_model::api::{
    BlobWithStatus, TransactionStatusDb, TransactionTypeDb, TransactionWithBlobs,
};
use hyle_model::utils::TimestampMs;
use hyle_modules::log_error;
use hyle_modules::{
    bus::SharedMessageBus,
    module_handle_messages,
    modules::{module_bus_client, Module, SharedBuildApiCtx},
};
use hyle_net::logged_task::logged_task;
use sqlx::Row;
use sqlx::{postgres::PgPoolOptions, PgPool, Pool, Postgres};
use std::collections::HashMap;
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

module_bus_client! {
#[derive(Debug)]
struct ExplorerBusClient {
    receiver(WsExplorerBlobTx),
}
}

#[derive(Debug, Clone)]
pub struct WsExplorerBlobTx {
    pub tx: BlobTransaction,
    pub tx_hash: TxHashDb,
    pub dp_hash: DataProposalHashDb,
    pub block_hash: ConsensusProposalHash,
    pub index: u32,
    pub version: u32,
    pub lane_id: Option<LaneId>,
    pub timestamp: Option<TimestampMs>,
}

// TODO: generalize for all tx types
type Subscribers = HashMap<ContractName, Vec<broadcast::Sender<TransactionWithBlobs>>>;

#[derive(Debug, Clone)]
pub struct ExplorerApiState {
    db: PgPool,
    new_sub_sender: mpsc::Sender<(ContractName, WebSocket)>,
}

#[derive(Debug)]
pub struct Explorer {
    bus: ExplorerBusClient,
    state: ExplorerApiState,
    pub(crate) new_sub_receiver: tokio::sync::mpsc::Receiver<(ContractName, WebSocket)>,
    subscribers: Subscribers,
}

impl Explorer {
    pub async fn new(bus: SharedMessageBus, db: Pool<Postgres>) -> Self {
        let (new_sub_sender, new_sub_receiver) = tokio::sync::mpsc::channel(100);
        Self {
            bus: ExplorerBusClient::new_from_bus(bus.new_handle()).await,
            state: ExplorerApiState { db, new_sub_sender },
            new_sub_receiver,
            subscribers: HashMap::new(),
        }
    }
}

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./src/indexer/migrations");

impl Module for Explorer {
    type Context = (SharedConf, SharedBuildApiCtx);

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect(&ctx.0.database_url)
            .await
            .context("Failed to connect to the database")?;

        tokio::time::timeout(tokio::time::Duration::from_secs(60), MIGRATOR.run(&pool)).await??;

        let explorer = Explorer::new(bus, pool).await;

        if let Ok(mut guard) = ctx.1.openapi.lock() {
            tracing::info!("Adding OpenAPI for Indexer");
            let openapi = guard.clone().nest("/v1/indexer", IndexerAPI::openapi());
            *guard = openapi;
        } else {
            tracing::error!("Failed to add OpenAPI for Indexer");
        }

        if let Ok(mut guard) = ctx.1.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.nest("/v1/indexer", explorer.api(Some(&ctx.1))));
                return Ok(explorer);
            }
        }

        anyhow::bail!("context router should be available");
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }
}

impl Explorer {
    pub async fn start(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<WsExplorerBlobTx> info => {
                self.send_blob_transaction_to_websocket_subscribers(
                    info
                );
            }

            Some((contract_name, socket)) = self.new_sub_receiver.recv() => {

                let (tx, mut rx) = broadcast::channel(100);
                // Append tx to the list of subscribers for contract_name
                self.subscribers.entry(contract_name)
                    .or_default()
                    .push(tx);

                logged_task(async move {
                        let (mut ws_tx, mut ws_rx) = socket.split();

                        loop {
                            select! {
                                maybe_transaction = rx.recv() => {
                                    match maybe_transaction {
                                        Ok(transaction) => {
                                            if let Ok(json) = log_error!(serde_json::to_vec(&transaction),
                                                "Serialize transaction to JSON") {
                                                if ws_tx.send(Message::Binary(json.into())).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                        _ => break,
                                    }
                                },
                                // Branch to handle incoming messages from ws
                                maybe_msg = ws_rx.next() => {
                                    match maybe_msg {
                                        Some(Ok(message)) => {
                                            if let Message::Close(frame) = message {
                                                info!("WS closed by client: {:?}", frame);
                                                let _ = ws_tx.send(Message::Close(frame)).await;
                                                break;
                                            }
                                        }
                                        Some(Err(e)) => {
                                            error!("Error while getting message from WS: {}", e);
                                            break;
                                        }
                                        None => break,
                                    }
                                }
                            }
                        }
                    });
            }


        };
        Ok(())
    }

    pub async fn get_last_block(&self) -> Result<Option<BlockHeight>> {
        let rows = sqlx::query("SELECT max(height) as max FROM blocks")
            .fetch_one(&self.state.db)
            .await?;
        Ok(rows
            .try_get("max")
            .map(|m: i64| Some(BlockHeight(m as u64)))
            .unwrap_or(None))
    }

    pub fn api(&self, ctx: Option<&SharedBuildApiCtx>) -> Router<()> {
        #[derive(OpenApi)]
        struct IndexerAPI;

        let (router, api) = OpenApiRouter::with_openapi(IndexerAPI::openapi())
            // stats
            .routes(routes!(api::get_stats))
            .routes(routes!(api::get_proof_stats))
            // block
            .routes(routes!(api::get_blocks))
            .routes(routes!(api::get_last_block))
            .routes(routes!(api::get_block))
            .routes(routes!(api::get_block_by_hash))
            // transaction
            .routes(routes!(api::get_transactions))
            .routes(routes!(api::get_transactions_by_height))
            .routes(routes!(api::get_transactions_by_contract))
            .routes(routes!(api::get_transaction_with_hash))
            .routes(routes!(api::get_transaction_events))
            .routes(routes!(api::get_blob_transactions_by_contract))
            .route(
                "/blob_transactions/contract/{contract_name}/ws",
                get(Self::get_blob_transactions_by_contract_ws_handler),
            )
            // proof transaction
            .routes(routes!(api::get_proofs))
            .routes(routes!(api::get_proofs_by_height))
            .routes(routes!(api::get_proof_with_hash))
            // blob
            .routes(routes!(api::get_blobs_by_tx_hash))
            .routes(routes!(api::get_blob))
            // contract
            .routes(routes!(api::list_contracts))
            .routes(routes!(api::get_contract))
            .routes(routes!(api::get_contract_state_by_height))
            .split_for_parts();

        if let Some(ctx) = ctx {
            if let Ok(mut o) = ctx.openapi.lock() {
                *o = o.clone().nest("/v1/indexer", api);
            }
        }

        router.with_state(self.state.clone())
    }

    async fn get_blob_transactions_by_contract_ws_handler(
        ws: WebSocketUpgrade,
        Path(contract_name): Path<String>,
        State(state): State<ExplorerApiState>,
    ) -> impl IntoResponse {
        ws.on_upgrade(move |socket| {
            Self::get_blob_transactions_by_contract_ws(socket, contract_name, state.new_sub_sender)
        })
    }

    async fn get_blob_transactions_by_contract_ws(
        socket: WebSocket,
        contract_name: String,
        new_sub_sender: mpsc::Sender<(ContractName, WebSocket)>,
    ) {
        // TODO: properly handle errors and ws messages
        _ = new_sub_sender
            .send((ContractName(contract_name), socket))
            .await;
    }

    fn send_blob_transaction_to_websocket_subscribers(&self, info: WsExplorerBlobTx) {
        let WsExplorerBlobTx {
            tx,
            tx_hash,
            dp_hash,
            block_hash,
            index,
            version,
            lane_id,
            timestamp,
        } = info;

        for (contrat_name, senders) in self.subscribers.iter() {
            if tx
                .blobs
                .iter()
                .any(|blob| &blob.contract_name == contrat_name)
            {
                let enriched_tx = TransactionWithBlobs {
                    tx_hash: tx_hash.0.clone(),
                    parent_dp_hash: dp_hash.0.clone(),
                    block_hash: block_hash.clone(),
                    index,
                    version,
                    transaction_type: TransactionTypeDb::BlobTransaction,
                    transaction_status: TransactionStatusDb::Sequenced,
                    lane_id: lane_id.clone(),
                    timestamp: timestamp.clone(),
                    identity: tx.identity.0.clone(),
                    blobs: tx
                        .blobs
                        .iter()
                        .map(|blob| BlobWithStatus {
                            contract_name: blob.contract_name.0.clone(),
                            data: blob.data.0.clone(),
                            proof_outputs: vec![],
                        })
                        .collect(),
                };
                senders.iter().for_each(|sender| {
                    let _ = sender.send(enriched_tx.clone());
                });
            }
        }
    }
}

impl std::ops::Deref for Explorer {
    type Target = Pool<Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.state.db
    }
}
