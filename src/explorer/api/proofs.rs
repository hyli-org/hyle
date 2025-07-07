use super::{BlockPagination, ExplorerApiState, TransactionDb};
use api::APITransaction;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use hyle_model::{api::APIProofDetails, utils::TimestampMs};
use sqlx::FromRow;
use sqlx::Row;

use crate::model::*;
use hyle_modules::log_error;

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/proofs",
    responses(
        (status = OK, body = [APITransaction])
    )
)]
pub async fn get_proofs(
    Query(pagination): Query<BlockPagination>,
    State(state): State<ExplorerApiState>,
) -> Result<Json<Vec<APITransaction>>, StatusCode> {
    let transactions = log_error!(
        match pagination.start_block {
            Some(start_block) => sqlx::query_as::<_, TransactionDb>(
                r#"
            SELECT t.*, b.timestamp
            FROM transactions t
            LEFT JOIN blocks b ON t.block_hash = b.hash
            WHERE b.height <= $1 and b.height > $2 AND t.transaction_type = 'proof_transaction'
            ORDER BY b.height DESC, t.index DESC
            LIMIT $3
            "#,
            )
            .bind(start_block)
            .bind(start_block - pagination.nb_results.unwrap_or(10)) // Fine if this goes negative
            .bind(pagination.nb_results.unwrap_or(10)),
            None => sqlx::query_as::<_, TransactionDb>(
                r#"
            SELECT t.*, b.timestamp
            FROM transactions t
            LEFT JOIN blocks b ON t.block_hash = b.hash
            WHERE t.transaction_type = 'proof_transaction'
            ORDER BY b.height DESC, t.index DESC
            LIMIT $1
            "#,
            )
            .bind(pagination.nb_results.unwrap_or(10)),
        }
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APITransaction>::into).collect()),
        "Failed to fetch proofs"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(transactions))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("height" = String, Path, description = "Block height")
    ),
    path = "/proofs/block/{height}",
    responses(
        (status = OK, body = [APITransaction])
    )
)]
pub async fn get_proofs_by_height(
    Path(height): Path<i64>,
    State(state): State<ExplorerApiState>,
) -> Result<Json<Vec<APITransaction>>, StatusCode> {
    let transactions = log_error!(
        sqlx::query_as::<_, TransactionDb>(
            r#"
        SELECT t.*, b.timestamp
        FROM transactions t
        JOIN blocks b ON t.block_hash = b.hash
        WHERE b.height = $1 AND t.transaction_type = 'proof_transaction'
        ORDER BY t.index DESC
        "#,
        )
        .bind(height)
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APITransaction>::into).collect()),
        "Failed to fetch proofs by height"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(transactions))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("tx_hash" = String, Path, description = "Tx hash")
    ),
    path = "/proof/hash/{tx_hash}",
    responses(
        (status = OK, body = APITransaction)
    )
)]
pub async fn get_proof_with_hash(
    Path(tx_hash): Path<String>,
    State(state): State<ExplorerApiState>,
) -> Result<Json<APIProofDetails>, StatusCode> {
    let transaction: Result<APIProofDetails, sqlx::Error> = log_error!(
        sqlx::query(
            r#"
SELECT 
    t.tx_hash,
    t.parent_dp_hash,
    t.block_hash,
    t.index,
    t.version,
    t.transaction_type,
    t.transaction_status,
    b.timestamp,
    t.lane_id,
    array_remove(ARRAY_AGG(bpo.blob_tx_hash), NULL) AS blob_tx_hashes,
    array_remove(ARRAY_AGG(bpo.blob_index), NULL) AS blob_tx_indexes,
    array_remove(ARRAY_AGG(bpo.blob_proof_output_index), NULL) AS blob_proof_output_indexes,
    array_remove(ARRAY_AGG(bpo.hyle_output), NULL) AS proof_outputs
FROM transactions t
LEFT JOIN blocks b 
    ON t.block_hash = b.hash
LEFT JOIN blob_proof_outputs bpo
    ON bpo.proof_tx_hash = t.tx_hash
    AND bpo.proof_parent_dp_hash = t.parent_dp_hash
WHERE 
    t.tx_hash = $1
    AND t.transaction_type = 'proof_transaction'
GROUP BY t.parent_dp_hash, t.tx_hash, b.height, b.timestamp
ORDER BY b.height DESC, t.index DESC
LIMIT 1;
"#,
        )
        .bind(tx_hash)
        .fetch_optional(&state.db)
        .await
        .map(|row| {
            let row = row.ok_or(sqlx::Error::RowNotFound)?;
            let api_tx: TransactionDb = FromRow::from_row(&row)?;
            let blob_tx_hashes: Vec<String> = row.try_get("blob_tx_hashes")?;
            let blob_tx_indexes: Vec<i32> = row.try_get("blob_tx_indexes")?;
            let blob_proof_output_indexes: Vec<i32> = row.try_get("blob_proof_output_indexes")?;
            let proof_outputs: Vec<serde_json::Value> = row.try_get("proof_outputs")?;

            let proof_outputs: Vec<(TxHash, u32, u32, serde_json::Value)> = blob_tx_hashes
                .into_iter()
                .zip(blob_tx_indexes)
                .zip(blob_proof_output_indexes)
                .zip(proof_outputs)
                .map(|(((tx_hash, tx_idx), proof_idx), output)| {
                    (tx_hash.into(), tx_idx as u32, proof_idx as u32, output)
                })
                .collect();

            Ok(APIProofDetails {
                tx_hash: api_tx.tx_hash.0,
                parent_dp_hash: api_tx.parent_dp_hash,
                block_hash: api_tx.block_hash,
                index: api_tx.index,
                version: api_tx.version,
                transaction_type: api_tx.transaction_type,
                transaction_status: api_tx.transaction_status,
                timestamp: api_tx
                    .timestamp
                    .map(|t| TimestampMs(t.and_utc().timestamp_millis() as u128)),
                lane_id: api_tx.lane_id.map(|l| l.0),
                proof_outputs,
            })
        }),
        "Failed to fetch proof by hash"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match transaction {
        Ok(tx) => Ok(Json(tx)),
        Err(_) => Err(StatusCode::NOT_FOUND),
    }
}
