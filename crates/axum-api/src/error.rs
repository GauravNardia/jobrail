use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Unauthorized(String),
    Forbidden(String),
    NotFound(String),
    Conflict(String),
    UnprocessableEntity(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, "BAD_REQUEST", message),

            Self::Unauthorized(message) => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED", message),

            Self::Forbidden(message) => (StatusCode::FORBIDDEN, "FORBIDDEN", message),

            Self::NotFound(message) => (StatusCode::NOT_FOUND, "NOT_FOUND", message),

            Self::Conflict(message) => (StatusCode::CONFLICT, "CONFLICT", message),

            Self::UnprocessableEntity(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "UNPROCESSABLE_ENTITY",
                message,
            ),

            Self::Internal(message) => {
                eprintln!("Internal API error: {message}");

                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_SERVER_ERROR",
                    "internal server error".to_string(),
                )
            }
        };

        (
            status,
            Json(ErrorResponse {
                error: ErrorBody { code, message },
            }),
        )
            .into_response()
    }
}
