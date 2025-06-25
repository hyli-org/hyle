//! Public API for interacting with the node.

use crate::{
    bus::{metrics::BusMetrics, BusClientSender, SharedMessageBus},
    log_error, module_bus_client, module_handle_messages,
    modules::{signal::ShutdownModule, Module},
};
use anyhow::{Context, Result};
pub use axum::Router;
use axum::{
    extract::{DefaultBodyLimit, Query, State},
    http::{header, Response, StatusCode},
    response::IntoResponse,
    routing::post,
};
use sdk::*;
use tokio_util::sync::CancellationToken;
use tower_http::catch_panic::CatchPanicLayer;
use tracing::info;

pub use client_sdk::contract_indexer::AppError;
pub use client_sdk::rest_client as client;

use super::{
    rest::request_logger,
    signal::{self, PersistModule},
};

module_bus_client! {
    struct AdminBusClient {
        sender(signal::PersistModule),
    }
}

pub struct AdminApiRunContext {
    pub port: u16,
    pub router: Router,
    pub max_body_size: usize,
}

impl AdminApiRunContext {
    pub fn new(port: u16, router: Router, max_body_size: usize) -> AdminApiRunContext {
        Self {
            port,
            router,
            max_body_size,
        }
    }
}

pub struct AdminApi {
    port: u16,
    app: Option<Router>,
    bus: AdminBusClient,
}

impl Module for AdminApi {
    type Context = AdminApiRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let app = ctx.router.merge(
            Router::new()
                .route("/v1/admin/persist", post(persist))
                .route("/v1/admin/inspect", post(inspect))
                .with_state(RouterState {
                    bus: AdminBusClient::new_from_bus(bus.new_handle()).await,
                }),
        );
        let app = app
            .layer(CatchPanicLayer::custom(handle_panic))
            .layer(DefaultBodyLimit::max(ctx.max_body_size)) // 10 MB
            .layer(tower_http::cors::CorsLayer::permissive())
            .layer(axum::middleware::from_fn(request_logger));
        Ok(AdminApi {
            port: ctx.port,
            app: Some(app),
            bus: AdminBusClient::new_from_bus(bus.new_handle()).await,
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.serve().await
    }
}

pub async fn persist(State(mut state): State<RouterState>) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Persisting modules state");
    state
        .bus
        .send(signal::PersistModule {})
        .context("Sending persist signal")?;
    Ok(())
}

struct JsonState {
    module_name: String,
}
pub async fn inspect(
    State(state): State<RouterState>,
    Query(json_state): Query<JsonState>,
) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Inspecting modules state");
    match json_state.module_name.as_str() {
        "node_state" => {
            state.bus.await.context("Requesting node state")?;

            Ok(serialize_modules(modules)?)
        }
        _ => Err(AppError::NotFound(format!(
            "Module {} not found",
            json_state.module_name
        ))),
    }
    let response = serde_json::to_string(&modules).context("Serializing modules")?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(response)
        .unwrap())
}

pub struct RouterState {
    bus: AdminBusClient,
}

impl AdminApi {
    pub async fn serve(&mut self) -> Result<()> {
        info!(
            "📡  Starting {} module, listening on port {}",
            std::any::type_name::<Self>(),
            self.port
        );

        let listener = hyle_net::net::bind_tcp_listener(self.port)
            .await
            .context("Starting rest server")?;

        #[allow(
            clippy::expect_used,
            reason = "app is guaranteed to be set during initialization"
        )]
        let app = self.app.take().expect("app is not set");

        // On module shutdown, we want to shutdown the axum server and wait for its shutdown to complete.
        let axum_cancel_token = CancellationToken::new();
        let axum_server = tokio::spawn({
            let token = axum_cancel_token.clone();
            async move {
                log_error!(
                    axum::serve(listener, app)
                        .with_graceful_shutdown(async move {
                            token.cancelled().await;
                        })
                        .await,
                    "serving Axum"
                )?;
                Ok::<(), anyhow::Error>(())
            }
        });
        module_handle_messages! {
            on_self self,
            delay_shutdown_until {
                // When the module tries to shutdown it'll cancel the token
                // and then we actually exit the loop when axum is done.
                axum_cancel_token.cancel();
                axum_server.is_finished()
            },
        };

        Ok(())
    }
}

impl Clone for RouterState {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
        }
    }
}

impl Clone for AdminBusClient {
    fn clone(&self) -> AdminBusClient {
        use crate::utils::static_type_map::Pick;

        AdminBusClient::new(
            Pick::<BusMetrics>::get(self).clone(),
            Pick::<tokio::sync::broadcast::Sender<PersistModule>>::get(self).clone(),
            Pick::<tokio::sync::broadcast::Receiver<ShutdownModule>>::get(self).resubscribe(),
            Pick::<tokio::sync::broadcast::Receiver<PersistModule>>::get(self).resubscribe(),
        )
    }
}

fn handle_panic(err: Box<dyn std::any::Any + Send + 'static>) -> Response<String> {
    let details = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "Unknown panic message".to_string()
    };

    tracing::error!("Panic occurred in Axum route: {}", details);

    let body = serde_json::json!({
        "error": {
            "kind": "panic",
            "details": details,
        }
    });
    let body = serde_json::to_string(&body).unwrap();

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}
