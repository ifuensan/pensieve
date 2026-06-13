//! Bulk data export endpoint.
//!
//! Streams raw Nostr events from ClickHouse for a fixed date range, straight to
//! the client as a download. Unlike the `/stats/*` endpoints this is *not*
//! cached or buffered: the response body is streamed chunk-by-chunk from
//! ClickHouse (`fetch_bytes`) so multi-GB exports never sit in memory.
//!
//! Ranges are a closed set mapped to server-side `WHERE` predicates — the
//! client never supplies raw SQL.

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::header;
use axum::response::Response;
use serde::Deserialize;
use tokio_util::io::ReaderStream;

use crate::error::ApiError;
use crate::state::AppState;

/// Query parameters for `GET /api/v1/export`.
#[derive(Debug, Deserialize)]
pub struct ExportQuery {
    /// Date range key (see [`range_predicate`]).
    pub range: String,
    /// Output format: `jsonl` (default) or `parquet`.
    pub format: Option<String>,
}

/// Map a fixed range key to a ClickHouse `WHERE` predicate on `created_at`.
///
/// Filtering is by **event** time (`created_at`), not ingestion time, so the
/// ranges mean what a researcher expects regardless of backfill/negentropy.
fn range_predicate(range: &str) -> Option<&'static str> {
    match range {
        "today" => Some("created_at >= toStartOfDay(now())"),
        "this_week" => Some("created_at >= toMonday(today())"),
        "this_month" => Some("created_at >= toStartOfMonth(today())"),
        "last_month" => Some(
            "created_at >= toStartOfMonth(today()) - INTERVAL 1 MONTH \
             AND created_at < toStartOfMonth(today())",
        ),
        "last_3_months" => Some("created_at >= today() - INTERVAL 3 MONTH"),
        _ => None,
    }
}

/// `GET /api/v1/export?range=<range>&format=<jsonl|parquet>`
///
/// Streams the matching events as a file download. Auth is enforced by the
/// shared middleware (Bearer header or `?token=` for browser/Grafana links).
pub async fn export(
    State(state): State<AppState>,
    Query(params): Query<ExportQuery>,
) -> Result<Response, ApiError> {
    let predicate = range_predicate(&params.range).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "invalid range '{}'. Valid: today, this_week, this_month, last_month, last_3_months",
            params.range
        ))
    })?;

    let (ch_format, ext, content_type) = match params.format.as_deref().unwrap_or("jsonl") {
        "jsonl" => ("JSONEachRow", "jsonl", "application/x-ndjson"),
        "parquet" => ("Parquet", "parquet", "application/vnd.apache.parquet"),
        other => {
            return Err(ApiError::BadRequest(format!(
                "invalid format '{other}'. Valid: jsonl, parquet"
            )));
        }
    };

    // Canonical Nostr event fields; created_at emitted as a unix timestamp (int),
    // the shape researchers expect.
    //
    // The date filter runs in a subquery on the raw `created_at` (DateTime): the
    // outer `toUnixTimestamp(created_at) AS created_at` would otherwise shadow the
    // column with a UInt32 alias, and the Date-returning predicates
    // (`toStartOfMonth(today())`, ...) then fail with "Illegal types (UInt32,
    // Date)". Filtering before the rename keeps every range working.
    //
    // No `FINAL`: on the HDD-backed deployment FINAL is ~40x slower (merge-on-read
    // across all parts) and `events_local` is a ReplacingMergeTree whose background
    // merges already keep it deduplicated in practice. A plain scan streams the
    // full range in seconds; at worst a handful of not-yet-merged rows repeat, and
    // the event `id` lets a consumer dedupe if it needs strict uniqueness.
    // No ORDER BY: avoids a full sort so large ranges stream with low memory.
    //
    // SETTINGS cap the blast radius of a large export on this co-located,
    // HDD-backed host: `max_threads` leaves cores for the ingester,
    // `max_memory_usage` is a backstop against a pathological range.
    let sql = format!(
        "SELECT id, pubkey, toUnixTimestamp(created_at) AS created_at, kind, tags, content, sig \
         FROM ( \
             SELECT id, pubkey, created_at, kind, tags, content, sig \
             FROM events_local \
             WHERE {predicate} \
         ) \
         SETTINGS max_threads = 4, max_memory_usage = 8000000000"
    );

    let cursor = state.clickhouse.query(&sql).fetch_bytes(ch_format)?;
    let body = Body::from_stream(ReaderStream::new(cursor));

    let filename = format!("pensieve-{}.{ext}", params.range);
    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .header(header::CACHE_CONTROL, "no-store")
        .body(body)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!(e)))
}
