use super::{BlockPagination, DataProposalHashDb, IndexerApiState, TransactionDb};
use api::{APIDataProposal, APITransaction};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use sqlx::types::chrono::NaiveDateTime;

use crate::model::*;
use hyle_modules::log_error;

#[derive(sqlx::FromRow, Debug)]
pub struct DataProposalDb {
    // Struct for the data_proposals table
    pub hash: DataProposalHashDb,
    pub parent_hash: Option<DataProposalHashDb>,
    pub lane_id: String,
    #[sqlx(try_from = "i32")]
    pub tx_count: u32,
    #[sqlx(try_from = "i64")]
    pub estimated_size: u64,
    pub block_hash: ConsensusProposalHash,
    #[sqlx(try_from = "i64")]
    pub block_height: u64,
    pub created_at: NaiveDateTime,
}

impl From<DataProposalDb> for APIDataProposal {
    fn from(value: DataProposalDb) -> Self {
        APIDataProposal {
            hash: value.hash.0,
            parent_hash: value.parent_hash.map(|h| h.0),
            lane_id: value.lane_id,
            tx_count: value.tx_count,
            estimated_size: value.estimated_size,
            block_hash: value.block_hash,
            block_height: value.block_height,
            timestamp: value.created_at.and_utc().timestamp_millis(),
        }
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/data_proposals/lane/{lane_id}",
    params(
        ("lane_id" = String, Path, description = "Lane ID")
    ),
    responses(
        (status = OK, body = [APIDataProposal])
    )
)]
pub async fn get_data_proposals_by_lane(
    Path(lane_id): Path<String>,
    Query(pagination): Query<BlockPagination>,
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<APIDataProposal>>, StatusCode> {
    let data_proposals = log_error!(
        sqlx::query_as::<_, DataProposalDb>(
            "SELECT * FROM data_proposals WHERE lane_id = $1 ORDER BY block_height DESC LIMIT $2"
        )
        .bind(lane_id)
        .bind(pagination.nb_results.unwrap_or(10))
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APIDataProposal>::into).collect()),
        "Failed to fetch data proposals by lane"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(data_proposals))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/data_proposal/hash/{hash}/transactions",
    params(
        ("hash" = String, Path, description = "Data proposal hash")
    ),
    responses(
        (status = OK, body = [APITransaction])
    )
)]
pub async fn get_data_proposal_by_hash(
    Path(hash): Path<String>,
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<APITransaction>>, StatusCode> {
    let transactions = log_error!(
        sqlx::query_as::<_, TransactionDb>(
            r#"
            SELECT t.*, b.timestamp
            FROM transactions t
            LEFT JOIN blocks b ON t.block_hash = b.hash
            WHERE t.dp_hash = $1
            ORDER BY b.height DESC, t.index DESC
            "#
        )
        .bind(hash)
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APITransaction>::into).collect()),
        "Failed to fetch transactions by data proposal hash"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(transactions))
}
