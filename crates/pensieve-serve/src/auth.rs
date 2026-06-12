//! Bearer token authentication middleware.

use axum::extract::Request;
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::Response;

use crate::error::ApiError;
use crate::state::AppState;

/// Middleware that requires a valid Bearer token in the `Authorization` header:
/// ```text
/// Authorization: Bearer <token>
/// ```
///
/// Tokens are validated against the list configured in `PENSIEVE_API_TOKENS`.
pub async fn require_auth(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    authorize(&state, &request, false)?;
    Ok(next.run(request).await)
}

/// Like [`require_auth`], but also accepts the token as a `?token=<token>`
/// query parameter.
///
/// This is scoped to the bulk-export download route only: plain browser links
/// (e.g. the download buttons embedded in Grafana) cannot set request headers.
/// It is deliberately *not* used for the other endpoints, so the rest of the
/// API stays header-only and tokens don't leak into access logs / history for
/// requests that don't need it.
pub async fn require_auth_allow_query_token(
    axum::extract::State(state): axum::extract::State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    authorize(&state, &request, true)?;
    Ok(next.run(request).await)
}

/// Validate the request's token. With `allow_query_token`, falls back to the
/// `?token=` query parameter when no Bearer header is present.
fn authorize(state: &AppState, request: &Request, allow_query_token: bool) -> Result<(), ApiError> {
    let token = bearer_token(request).or_else(|| match allow_query_token {
        true => query_token(request),
        false => None,
    });

    match token {
        Some(token) if token_is_valid(&state.config.api_tokens, &token) => Ok(()),
        Some(_) => {
            tracing::debug!("invalid api token");
            Err(ApiError::Unauthorized)
        }
        None => {
            tracing::debug!("missing token");
            Err(ApiError::Unauthorized)
        }
    }
}

/// Extract a Bearer token from the `Authorization` header, if present.
fn bearer_token(request: &Request) -> Option<String> {
    request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
        .map(|token| token.to_string())
}

/// Extract a `token` value from the URL query string, if present.
///
/// API tokens are hex (`openssl rand -hex 32`), so no percent-decoding is
/// needed for a valid token; an encoded value simply won't match.
fn query_token(request: &Request) -> Option<String> {
    request
        .uri()
        .query()?
        .split('&')
        .find_map(|pair| pair.strip_prefix("token=").map(|v| v.to_string()))
}

/// Constant-time check that `token` matches one of the configured tokens.
///
/// Iterates the entire set without early exit and compares each candidate with a
/// constant-time byte comparison, so response timing does not reveal how many
/// leading characters of a guessed token were correct (which a naive
/// `HashSet::contains` / `==` would leak).
fn token_is_valid(tokens: &std::collections::HashSet<String>, token: &str) -> bool {
    let mut valid = false;
    for known in tokens {
        // Bitwise `|=` (not `||`) so the comparison runs for every token.
        valid |= constant_time_eq(known.as_bytes(), token.as_bytes());
    }
    valid
}

/// Constant-time equality for two byte slices.
///
/// Returns early only on a length mismatch (token length is not a meaningful
/// secret); for equal lengths the comparison time is independent of the content.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
