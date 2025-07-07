use super::{BlockPagination, ExplorerApiState};
use api::APIBlock;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use sqlx::types::chrono::NaiveDateTime;

use crate::model::*;
use hyle_modules::log_error;

#[derive(sqlx::FromRow, Debug)]
pub struct BlockDb {
    // Struct for the blocks table
    pub hash: ConsensusProposalHash,
    pub parent_hash: ConsensusProposalHash,
    #[sqlx(try_from = "i64")]
    pub height: u64, // Corresponds to BlockHeight
    pub timestamp: NaiveDateTime, // UNIX timestamp
    #[sqlx(try_from = "i64")]
    pub total_txs: u64, // Total number of transactions in the block
}

impl From<BlockDb> for APIBlock {
    fn from(value: BlockDb) -> Self {
        APIBlock {
            hash: value.hash,
            parent_hash: value.parent_hash,
            height: value.height,
            timestamp: value.timestamp.and_utc().timestamp_millis(),
            total_txs: value.total_txs,
        }
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/blocks",
    responses(
        (status = OK, body = [APIBlock])
    )
)]
pub async fn get_blocks(
    Query(pagination): Query<BlockPagination>,
    State(state): State<ExplorerApiState>,
) -> Result<Json<Vec<APIBlock>>, StatusCode> {
    let blocks = log_error!(match pagination.start_block {
        Some(start_block) => sqlx::query_as::<_, BlockDb>(
            "SELECT * FROM blocks WHERE height <= $1 and height > $2 ORDER BY height DESC LIMIT $3",
        )
        .bind(start_block)
        .bind(start_block - pagination.nb_results.unwrap_or(10)) // Fine if this goes negative
        .bind(pagination.nb_results.unwrap_or(10)),
        None => sqlx::query_as::<_, BlockDb>("SELECT * FROM blocks ORDER BY height DESC LIMIT $1")
            .bind(pagination.nb_results.unwrap_or(10)),
    }
    .fetch_all(&state.db)
    .await
    .map(|db| db.into_iter().map(Into::<APIBlock>::into).collect()),
    "Failed to fetch blocks")
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(blocks))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/block/last",
    responses(
        (status = OK, body = APIBlock)
    )
)]
pub async fn get_last_block(
    State(state): State<ExplorerApiState>,
) -> Result<Json<APIBlock>, StatusCode> {
    let block = log_error!(
        sqlx::query_as::<_, BlockDb>("SELECT * FROM blocks ORDER BY height DESC LIMIT 1")
            .fetch_optional(&state.db)
            .await
            .map(|db| db.map(Into::<APIBlock>::into)),
        "Failed to fetch last block"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match block {
        Some(block) => Ok(Json(block)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/block/height/{height}",
    params(
        ("height" = String, Path, description = "Block height")
    ),
    responses(
        (status = OK, body = APIBlock)
    )
)]
pub async fn get_block(
    Path(height): Path<i64>,
    State(state): State<ExplorerApiState>,
) -> Result<Json<APIBlock>, StatusCode> {
    let block = log_error!(
        sqlx::query_as::<_, BlockDb>("SELECT * FROM blocks WHERE height = $1")
            .bind(height)
            .fetch_optional(&state.db)
            .await
            .map(|db| db.map(Into::<APIBlock>::into)),
        "Failed to fetch block by height"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match block {
        Some(block) => Ok(Json(block)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/block/hash/{hash}",
    params(
        ("hash" = String, Path, description = "Block hash"),
    ),
    responses(
        (status = OK, body = APIBlock)
    )
)]
pub async fn get_block_by_hash(
    Path(hash): Path<String>,
    State(state): State<ExplorerApiState>,
) -> Result<Json<APIBlock>, StatusCode> {
    let block = log_error!(
        sqlx::query_as::<_, BlockDb>("SELECT * FROM blocks WHERE hash = $1")
            .bind(hash)
            .fetch_optional(&state.db)
            .await
            .map(|db| db.map(Into::<APIBlock>::into)),
        "Failed to fetch block by hash"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match block {
        Some(block) => Ok(Json(block)),
        None => Err(StatusCode::NOT_FOUND),
    }
}
