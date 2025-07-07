use super::ExplorerApiState;
use axum::{extract::State, http::StatusCode, Json};
use hyle_model::api::{NetworkStats, ProofStat};

use hyle_modules::log_error;

#[derive(sqlx::FromRow, Debug)]
pub struct Point<T = i64> {
    pub x: i64,
    pub y: Option<T>,
}

#[derive(sqlx::FromRow, Debug)]
struct PeakStat {
    pub minute_bucket: i64,
    pub tx_count: i64,
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/stats",
    responses(
        (status = OK, body = NetworkStats)
    )
)]
pub async fn get_stats(
    State(state): State<ExplorerApiState>,
) -> Result<Json<NetworkStats>, StatusCode> {
    let total_transactions = log_error!(
        sqlx::query_scalar("SELECT count(*) as txs FROM transactions")
            .fetch_optional(&state.db)
            .await,
        "Failed to fetch stats"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .unwrap_or(0);

    let total_contracts = log_error!(
        sqlx::query_scalar("SELECT count(*) as contracts FROM contracts")
            .fetch_optional(&state.db)
            .await,
        "Failed to fetch stats"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .unwrap_or(0);

    let txs_last_day = log_error!(
        sqlx::query_scalar(
            "
            SELECT count(*) as txs FROM transactions
            LEFT JOIN blocks b ON transactions.block_hash = b.hash
            WHERE b.timestamp > now() - interval '1 day'
            OR b.timestamp IS NULL
            "
        )
        .fetch_optional(&state.db)
        .await,
        "Failed to fetch stats"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .unwrap_or(0);

    let contracts_last_day = log_error!(
        sqlx::query_scalar(
            "
            SELECT count(*) as contracts FROM contracts
            LEFT JOIN transactions t ON contracts.tx_hash = t.tx_hash
            LEFT JOIN blocks b ON t.block_hash = b.hash
            WHERE b.timestamp > now() - interval '1 day'
            OR b.timestamp IS NULL
            "
        )
        .fetch_optional(&state.db)
        .await,
        "Failed to fetch stats"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .unwrap_or(0);

    // graph is number of txs per hour for the last 6 hours
    let graph_tx_volume = log_error!(
        sqlx::query_as::<_, Point>(
            "
            WITH hours AS (
                SELECT generate_series(
                    date_trunc('hour', now()) - interval '5 hours',
                    date_trunc('hour', now()),
                    interval '1 hour'
                ) AS x
            )
            SELECT 
                extract(epoch from h.x)::bigint AS x,
                count(t.*)::bigint AS y
            FROM hours h
            LEFT JOIN blocks b ON date_trunc('hour', b.timestamp) = h.x
            LEFT JOIN transactions t ON t.block_hash = b.hash
            GROUP BY h.x
            ORDER BY h.x;
            "
        )
        .fetch_all(&state.db)
        .await,
        "Failed to fetch tx stats"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let graph_tx_volume = graph_tx_volume
        .into_iter()
        .map(|point| (point.x, point.y.unwrap_or(0)))
        .collect::<Vec<(i64, i64)>>();

    let graph_block_time = log_error!(
        sqlx::query_as::<_, Point<f64>>(
            "
            WITH hours AS (
                SELECT generate_series(
                    date_trunc('hour', now()) - interval '5 hours',
                    date_trunc('hour', now()),
                    interval '1 hour'
                ) AS hour_start
            ),
            block_deltas AS (
                SELECT
                    b.timestamp AS current_ts,
                    bp.timestamp AS parent_ts,
                    b.timestamp - bp.timestamp AS delta,
                    date_trunc('hour', b.timestamp) AS bucket
                FROM blocks b
                JOIN blocks bp ON b.parent_hash = bp.hash
                WHERE b.timestamp > now() - interval '6 hours'
                AND bp.height > 0
            )
            SELECT
                extract(epoch from h.hour_start)::bigint AS x,
                AVG(EXTRACT(EPOCH FROM bd.delta))::float8 AS y
            FROM hours h
            LEFT JOIN block_deltas bd ON bd.bucket = h.hour_start
            GROUP BY h.hour_start
            ORDER BY h.hour_start;
            "
        )
        .fetch_all(&state.db)
        .await,
        "Failed to fetch block stats"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let graph_block_time = graph_block_time
        .into_iter()
        .map(|point| (point.x, point.y.unwrap_or(0.)))
        .collect::<Vec<(i64, f64)>>();

    let peak_txs = log_error!(
        sqlx::query_as::<_, PeakStat>(
            "
            SELECT
              extract(epoch FROM date_trunc('minute', b.timestamp))::bigint  AS minute_bucket,
              count(*)::bigint                  AS tx_count
            FROM blocks b
            JOIN transactions t ON t.block_hash = b.hash
            WHERE b.timestamp >= now() - interval '24 hours'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 1;
            "
        )
        .fetch_optional(&state.db)
        .await,
        "Failed to fetch peak TPM"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .unwrap_or(PeakStat {
        minute_bucket: 0,
        tx_count: 0,
    });

    let peak_txs = (peak_txs.minute_bucket, peak_txs.tx_count);

    Ok(Json(NetworkStats {
        total_transactions,
        txs_last_day,
        total_contracts,
        contracts_last_day,
        graph_tx_volume,
        graph_block_time,
        peak_txs,
    }))
}

#[utoipa::path(
    get,
    tag = "Indexer",
    path = "/stats/proofs",
    responses(
        (status = OK, body = Vec<ProofStat>)
    )
)]
pub async fn get_proof_stats(
    State(state): State<ExplorerApiState>,
) -> Result<Json<Vec<ProofStat>>, StatusCode> {
    let transactions = log_error!(
        sqlx::query_as::<_, ProofStat>(
            r#"
SELECT
    c.verifier,
    COUNT(DISTINCT bpo.proof_tx_hash) AS proof_count
FROM blob_proof_outputs bpo
JOIN contracts c ON bpo.contract_name = c.contract_name
GROUP BY c.verifier
ORDER BY proof_count DESC;
        "#,
        )
        .fetch_all(&state.db)
        .await,
        "Failed to fetch proof stats"
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(transactions))
}
