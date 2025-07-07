use std::num::TryFromIntError;

use super::{BlockPagination, IndexerApiState};
use api::{
    APITransaction, APITransactionEvents, BlobWithStatus, TransactionStatusDb, TransactionTypeDb,
    TransactionWithBlobs,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use hyle_model::utils::TimestampMs;
use sqlx::postgres::PgRow;
use sqlx::types::chrono::NaiveDateTime;
use sqlx::FromRow;
use sqlx::Row;
use sqlx::{prelude::Type, Postgres};

use crate::model::*;
use hyle_modules::log_error;

#[derive(Debug, Clone, PartialEq)]
pub struct DataProposalHashDb(pub DataProposalHash);

impl From<DataProposalHash> for DataProposalHashDb {
    fn from(dp_hash: DataProposalHash) -> Self {
        DataProposalHashDb(dp_hash)
    }
}

impl Type<Postgres> for DataProposalHashDb {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }
}
impl sqlx::Encode<'_, sqlx::Postgres> for DataProposalHashDb {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<
        sqlx::encode::IsNull,
        std::boxed::Box<(dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static)>,
    > {
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0 .0, buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for DataProposalHashDb {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        DataProposalHashDb,
        std::boxed::Box<(dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static)>,
    > {
        let inner = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(DataProposalHashDb(DataProposalHash(inner)))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TxHashDb(pub TxHash);

impl From<TxHash> for TxHashDb {
    fn from(tx_hash: TxHash) -> Self {
        TxHashDb(tx_hash)
    }
}

impl Type<Postgres> for TxHashDb {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }
}
impl sqlx::Encode<'_, sqlx::Postgres> for TxHashDb {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<
        sqlx::encode::IsNull,
        std::boxed::Box<(dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static)>,
    > {
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0 .0, buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for TxHashDb {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        TxHashDb,
        std::boxed::Box<(dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static)>,
    > {
        let inner = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(TxHashDb(TxHash(inner)))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LaneIdDb(pub LaneId);

impl From<LaneId> for LaneIdDb {
    fn from(lane_id: LaneId) -> Self {
        LaneIdDb(lane_id)
    }
}

impl Type<Postgres> for LaneIdDb {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }
}
impl sqlx::Encode<'_, sqlx::Postgres> for LaneIdDb {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<
        sqlx::encode::IsNull,
        std::boxed::Box<(dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static)>,
    > {
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&hex::encode(&self.0 .0 .0), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for LaneIdDb {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        LaneIdDb,
        std::boxed::Box<(dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static)>,
    > {
        let inner = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(LaneIdDb(LaneId(ValidatorPublicKey(hex::decode(inner)?))))
    }
}

#[derive(Debug)]
pub struct TransactionDb {
    // Struct for the transactions table
    pub tx_hash: TxHashDb,                         // Transaction hash
    pub parent_dp_hash: DataProposalHash,          // Corresponds to the data proposal hash
    pub block_hash: Option<ConsensusProposalHash>, // Corresponds to the block hash
    pub index: Option<u32>,                        // Index of the transaction within the block
    pub version: u32,                              // Transaction version
    pub transaction_type: TransactionTypeDb,       // Type of transaction
    pub transaction_status: TransactionStatusDb,   // Status of the transaction
    pub timestamp: Option<NaiveDateTime>,          // Timestamp of the transaction (block timestamp)
    pub lane_id: Option<LaneIdDb>,                 // Lane ID of the transaction
    pub identity: Option<String>, // Identity of the transaction sender (null for proofs)
}

impl<'r> FromRow<'r, PgRow> for TransactionDb {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let tx_hash = row.try_get("tx_hash")?;
        let block_hash = row.try_get("block_hash")?;
        let dp_hash_db: DataProposalHashDb = row.try_get("parent_dp_hash")?;
        let parent_dp_hash = dp_hash_db.0;
        let index: Option<i32> = row.try_get("index")?;
        let version: i32 = row.try_get("version")?;
        let version: u32 = version
            .try_into()
            .map_err(|e: TryFromIntError| sqlx::Error::Decode(e.into()))?;
        let transaction_type: TransactionTypeDb = row.try_get("transaction_type")?;
        let transaction_status: TransactionStatusDb = row.try_get("transaction_status")?;
        let index = match index {
            None => None,
            Some(index) => Some(
                index
                    .try_into()
                    .map_err(|e: TryFromIntError| sqlx::Error::Decode(e.into()))?,
            ),
        };
        let timestamp: Option<NaiveDateTime> = row.try_get("timestamp").unwrap_or_default();
        let lane_id: Option<LaneIdDb> = row.try_get("lane_id").unwrap_or_default();
        let identity: Option<String> = row.try_get("identity").unwrap_or_default();

        Ok(TransactionDb {
            tx_hash,
            parent_dp_hash,
            block_hash,
            index,
            version,
            transaction_type,
            transaction_status,
            timestamp,
            lane_id,
            identity,
        })
    }
}

impl From<TransactionDb> for APITransaction {
    fn from(val: TransactionDb) -> Self {
        let timestamp = val
            .timestamp
            .map(|t| TimestampMs(t.and_utc().timestamp_millis() as u128));
        APITransaction {
            tx_hash: val.tx_hash.0,
            parent_dp_hash: val.parent_dp_hash,
            block_hash: val.block_hash,
            index: val.index,
            version: val.version,
            transaction_type: val.transaction_type,
            transaction_status: val.transaction_status,
            timestamp,
            lane_id: val.lane_id.map(|l| l.0),
            identity: val.identity,
        }
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/transactions",
    responses(
        (status = OK, body = [APITransaction])
    )
)]
pub async fn get_transactions(
    Query(pagination): Query<BlockPagination>,
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<APITransaction>>, StatusCode> {
    let transactions = log_error!(
        match pagination.start_block {
            Some(start_block) => sqlx::query_as::<_, TransactionDb>(
                r#"
            SELECT t.*, b.timestamp
            FROM transactions t
            LEFT JOIN blocks b ON t.block_hash = b.hash
            WHERE b.height <= $1 and b.height > $2 AND t.transaction_type = 'blob_transaction'
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
            WHERE t.transaction_type = 'blob_transaction'
            ORDER BY b.height DESC, t.index DESC
            LIMIT $1
            "#,
            )
            .bind(pagination.nb_results.unwrap_or(10)),
        }
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APITransaction>::into).collect()),
        "Failed to fetch transactions"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(transactions))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("contract_name" = String, Path, description = "Contract name"),
    ),
    path = "/transactions/contract/{contract_name}",
    responses(
        (status = OK, body = [APITransaction])
    )
)]
pub async fn get_transactions_by_contract(
    Path(contract_name): Path<String>,
    Query(pagination): Query<BlockPagination>,
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<APITransaction>>, StatusCode> {
    let transactions = log_error!(match pagination.start_block {
        Some(start_block) => sqlx::query_as::<_, TransactionDb>(
            r#"
            SELECT t.* , bl.timestamp
            FROM transactions t
            JOIN blobs b ON t.tx_hash = b.tx_hash
            LEFT JOIN blocks bl ON t.block_hash = bl.hash
            WHERE b.contract_name = $1 AND bl.height <= $2 AND bl.height > $3 AND t.transaction_type = 'blob_transaction'
            ORDER BY bl.height DESC, t.index DESC
            LIMIT $4
            "#,
        )
        .bind(contract_name)
        .bind(start_block)
        .bind(start_block - pagination.nb_results.unwrap_or(10)) // Fine if this goes negative
        .bind(pagination.nb_results.unwrap_or(10)),
        None => sqlx::query_as::<_, TransactionDb>(
            r#"
            SELECT t.*, bl.timestamp
            FROM transactions t
            JOIN blobs b ON t.tx_hash = b.tx_hash AND t.parent_dp_hash = b.parent_dp_hash
            LEFT JOIN blocks bl ON t.block_hash = bl.hash
            WHERE b.contract_name = $1 AND t.transaction_type = 'blob_transaction'
            ORDER BY bl.height DESC, t.index DESC
            LIMIT $2
            "#,
        )
        .bind(contract_name)
        .bind(pagination.nb_results.unwrap_or(10)),
    }
    .fetch_all(&state.db)
    .await
    .map(|db| db.into_iter().map(Into::<APITransaction>::into).collect()),
    "Failed to fetch transactions by contract")
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(transactions))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("height" = String, Path, description = "Block height")
    ),
    path = "/transactions/block/{height}",
    responses(
        (status = OK, body = [APITransaction])
    )
)]
pub async fn get_transactions_by_height(
    Path(height): Path<i64>,
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<APITransaction>>, StatusCode> {
    let transactions = log_error!(
        sqlx::query_as::<_, TransactionDb>(
            r#"
        SELECT t.*, b.timestamp
        FROM transactions t
        JOIN blocks b ON t.block_hash = b.hash
        WHERE b.height = $1 AND t.transaction_type = 'blob_transaction'
        ORDER BY t.index DESC
        "#,
        )
        .bind(height)
        .fetch_all(&state.db)
        .await
        .map(|db| db.into_iter().map(Into::<APITransaction>::into).collect()),
        "Failed to fetch transactions by height"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(transactions))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("tx_hash" = String, Path, description = "Tx hash"),
    ),
    path = "/transaction/hash/{tx_hash}",
    responses(
        (status = OK, body = APITransaction)
    )
)]
pub async fn get_transaction_with_hash(
    Path(tx_hash): Path<String>,
    State(state): State<IndexerApiState>,
) -> Result<Json<APITransaction>, StatusCode> {
    let transaction = log_error!(
        sqlx::query_as::<_, TransactionDb>(
            r#"
SELECT
    tx_hash,
    version,
    transaction_type,
    transaction_status,
    parent_dp_hash,
    block_hash,
    index,
    b.timestamp,
    lane_id,
    identity
FROM transactions t
LEFT JOIN blocks b ON t.block_hash = b.hash
WHERE t.tx_hash = $1 AND transaction_type='blob_transaction'
ORDER BY block_height DESC, index DESC
LIMIT 1;
        "#,
        )
        .bind(tx_hash)
        .fetch_optional(&state.db)
        .await
        .map(|db| db.map(|test| { Into::<APITransaction>::into(test) })),
        "Failed to fetch transaction by hash"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match transaction {
        Some(tx) => Ok(Json(tx)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("tx_hash" = String, Path, description = "Tx hash"),
    ),
    path = "/transaction/hash/{tx_hash}/events",
    responses(
        (status = OK, body = [APITransactionEvents])
    )
)]
pub async fn get_transaction_events(
    Path(tx_hash): Path<String>,
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<APITransactionEvents>>, StatusCode> {
    let rows = log_error!(
        sqlx::query(
            r#"
with filtered as (
    SELECT 
        t.block_hash,
        b.height,
        t.tx_hash,
        t.parent_dp_hash,
        t.events,
        t.index
    FROM transaction_state_events t
    LEFT JOIN blocks b 
        ON t.block_hash = b.hash
    WHERE 
        t.tx_hash = $1
        AND t.block_height = b.height
)
SELECT
    block_hash,
    height,
    tx_hash,
    events
FROM
    (SELECT filtered.*, parent_dp_hash = FIRST_VALUE(parent_dp_hash) OVER (ORDER BY height DESC, index DESC) as first_res FROM filtered)
WHERE first_res = TRUE
ORDER BY 
    height DESC,
    index DESC;
"#,
        )
        .bind(tx_hash)
        .fetch_all(&state.db)
        .await,
        "Failed to fetch transaction events"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let transactions: Result<Vec<APITransactionEvents>, anyhow::Error> = rows
        .into_iter()
        .map(|row| {
            let block_hash = row.try_get("block_hash")?;
            let block_height: i64 = row.try_get("height")?;
            let block_height = BlockHeight(block_height.try_into()?);
            let events: serde_json::Value = row.try_get("events")?;
            let events: Vec<serde_json::Value> = serde_json::from_value(events)?;
            Ok(APITransactionEvents {
                block_hash,
                block_height,
                events,
            })
        })
        .collect();

    match transactions {
        Ok(transactions) => Ok(Json(transactions)),
        Err(e) => {
            tracing::warn!("Failed to parse transactions with blobs: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[utoipa::path(
    get,
    tag = "Indexer",
    params(
        ("contract_name" = String, Path, description = "Contract name"),
    ),
    path = "/blob_transactions/contract/{contract_name}",
    responses(
        (status = OK, body = [TransactionWithBlobs])
    )
)]
pub async fn get_blob_transactions_by_contract(
    Path(contract_name): Path<String>,
    State(state): State<IndexerApiState>,
) -> Result<Json<Vec<TransactionWithBlobs>>, StatusCode> {
    let rows = log_error!(sqlx::query(
        r#"
        with blobs as (
            SELECT blobs.*, array_remove(ARRAY_AGG(blob_proof_outputs.hyle_output), NULL) AS proof_outputs
            FROM blobs
            LEFT JOIN blob_proof_outputs ON blobs.parent_dp_hash = blob_proof_outputs.blob_parent_dp_hash AND blobs.tx_hash = blob_proof_outputs.blob_tx_hash AND blobs.blob_index = blob_proof_outputs.blob_index
            WHERE blobs.contract_name = $1
            GROUP BY blobs.parent_dp_hash, blobs.tx_hash, blobs.blob_index, blobs.identity
        )
        SELECT
            t.tx_hash,
            t.parent_dp_hash,
            t.block_hash,
            t.index,
            t.version,
            t.transaction_type,
            t.transaction_status,
            t.identity,
            array_agg(ROW(b.contract_name, b.data, b.proof_outputs)) AS blobs
        FROM blobs b
        JOIN transactions t on t.tx_hash = b.tx_hash AND t.parent_dp_hash = b.parent_dp_hash
        GROUP BY
            t.tx_hash,
            t.parent_dp_hash,
            t.block_hash,
            t.index,
            t.version,
            t.transaction_type,
            t.transaction_status,
            t.identity
        "#,
    )
    .bind(contract_name.clone())
    .fetch_all(&state.db)
    .await, "Failed to fetch blob transactions by contract")
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let transactions: Result<Vec<TransactionWithBlobs>, _> = rows
        .into_iter()
        .map(|row| {
            let api_tx: TransactionDb = FromRow::from_row(&row)?;
            let Some(block_hash) = api_tx.block_hash else {
                return Err(sqlx::Error::RowNotFound);
            };
            let blobs: Vec<(String, Vec<u8>, Vec<serde_json::Value>)> = row.try_get("blobs")?;
            let blobs = blobs
                .into_iter()
                .map(|(contract_name, data, proof_outputs)| BlobWithStatus {
                    contract_name,
                    data,
                    proof_outputs,
                })
                .collect();
            let Some(identity) = api_tx.identity else {
                return Err(sqlx::Error::RowNotFound);
            };

            Ok(TransactionWithBlobs {
                tx_hash: api_tx.tx_hash.0,
                parent_dp_hash: api_tx.parent_dp_hash,
                block_hash,
                index: api_tx.index.unwrap_or(0),
                version: api_tx.version,
                transaction_type: api_tx.transaction_type,
                transaction_status: api_tx.transaction_status,
                timestamp: api_tx
                    .timestamp
                    .map(|t| TimestampMs(t.and_utc().timestamp_millis() as u128)),
                lane_id: api_tx.lane_id.map(|l| l.0),
                identity,
                blobs,
            })
        })
        .collect();
    match transactions {
        Ok(transactions) => Ok(Json(transactions)),
        Err(e) => {
            tracing::warn!("Failed to parse transactions with blobs: {:?}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
