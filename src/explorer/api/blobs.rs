use super::{ExplorerApiState, TxHashDb};
use api::APIBlob;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};

use crate::model::*;
use hyle_modules::log_error;

#[derive(sqlx::FromRow, Debug)]
pub struct BlobDb {
    pub tx_hash: TxHashDb, // Corresponds to the transaction hash
    #[sqlx(try_from = "i32")]
    pub blob_index: u32, // Index of the blob within the transaction
    pub identity: String,  // Identity of the blob
    pub contract_name: String, // Contract name associated with the blob
    pub data: Vec<u8>,     // Actual blob data
    pub proof_outputs: Vec<serde_json::Value>, // outputs of proofs
    pub verified: bool,    // Verification status
}

impl From<BlobDb> for APIBlob {
    fn from(value: BlobDb) -> Self {
        APIBlob {
            tx_hash: value.tx_hash.0,
            blob_index: value.blob_index,
            identity: value.identity,
            contract_name: value.contract_name,
            data: value.data,
            proof_outputs: value.proof_outputs,
            verified: value.verified,
        }
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("tx_hash" = String, Path, description = "Tx hash"),
    ),
    path = "/blobs/hash/{tx_hash}",
    responses(
        (status = OK, body = [APIBlob])
    )
)]
pub async fn get_blobs_by_tx_hash(
    Path(tx_hash): Path<String>,
    State(state): State<ExplorerApiState>,
) -> Result<Json<Vec<APIBlob>>, StatusCode> {
    let blobs = log_error!(
        sqlx::query_as::<_, BlobDb>(
            r#"
WITH latest_height_for_this_tx_hash AS (
  SELECT MAX(block_height) as max_height
  FROM transactions
  WHERE tx_hash = $1
    AND transaction_type = 'blob_transaction'
)

SELECT 
      blobs.*,
      array_remove(ARRAY_AGG(blob_proof_outputs.hyle_output), NULL) AS proof_outputs
FROM blobs
LEFT JOIN
     blob_proof_outputs
	ON blobs.parent_dp_hash = blob_proof_outputs.blob_parent_dp_hash 
    	   AND blobs.tx_hash = blob_proof_outputs.blob_tx_hash 
    	   AND blobs.blob_index = blob_proof_outputs.blob_index
JOIN
     transactions
        ON transactions.parent_dp_hash = blobs.parent_dp_hash AND transactions.tx_hash = blobs.tx_hash
WHERE blobs.tx_hash = $1
     AND transactions.block_height = (SELECT max_height FROM latest_height_for_this_tx_hash)
GROUP BY
      blobs.parent_dp_hash,
      blobs.tx_hash,
      blobs.blob_index,
      blobs.identity
"#,
        )
        .bind(tx_hash)
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APIBlob>::into).collect()),
        "Failed to fetch blobs by tx hash"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(blobs))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("tx_hash" = String, Path, description = "Tx hash"),
        ("blob_index" = String, Path, description = "Blob index"),
    ),
    path = "/blob/hash/{tx_hash}/index/{blob_index}",
    responses(
        (status = OK, body = APIBlob)
    )
)]
pub async fn get_blob(
    Path((tx_hash, blob_index)): Path<(String, i32)>,
    State(state): State<ExplorerApiState>,
) -> Result<Json<APIBlob>, StatusCode> {
    let blob = log_error!(
        sqlx::query_as::<_, BlobDb>(
            r#"
SELECT 
  blobs.*, 
  array_remove(ARRAY_AGG(blob_proof_outputs.hyle_output), NULL) AS proof_outputs
FROM blobs
LEFT JOIN blob_proof_outputs 
  ON blobs.parent_dp_hash = blob_proof_outputs.blob_parent_dp_hash
  AND blobs.tx_hash = blob_proof_outputs.blob_tx_hash
  AND blobs.blob_index = blob_proof_outputs.blob_index
JOIN transactions
  ON blobs.parent_dp_hash = transactions.parent_dp_hash AND blobs.tx_hash = transactions.tx_hash
WHERE
  blobs.tx_hash = $1
  AND blobs.blob_index = $2
GROUP BY 
  blobs.parent_dp_hash, 
  blobs.tx_hash, 
  blobs.blob_index,
  transactions.block_height
ORDER BY transactions.block_height DESC
LIMIT 1;
"#,
        )
        .bind(tx_hash)
        .bind(blob_index)
        .fetch_optional(&state.db)
        .await
        .map(|db| db.map(Into::<APIBlob>::into)),
        "Failed to fetch blob"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match blob {
        Some(blob) => Ok(Json(blob)),
        None => Err(StatusCode::NOT_FOUND),
    }
}
