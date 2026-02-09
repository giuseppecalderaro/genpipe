use thiserror::Error;

/// Errors that can occur within the Helix pipeline framework.
#[derive(Error, Debug)]
pub enum Error {
    #[error("channel send failed: {0}")]
    ChannelSend(String),

    #[error("channel closed unexpectedly")]
    ChannelClosed,

    #[error("source `{0}`: {1}")]
    Source(String, String),

    #[error("processor `{0}`: {1}")]
    Processor(String, String),

    #[error("sink `{0}`: {1}")]
    Sink(String, String),

    #[error("service `{0}`: {1}")]
    Service(String, String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("msgpack encode error: {0}")]
    MsgPackEncode(#[from] rmp_serde::encode::Error),

    #[error("msgpack decode error: {0}")]
    MsgPackDecode(#[from] rmp_serde::decode::Error),

    #[error("nats error: {0}")]
    Nats(String),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// A convenience alias for `Result<T, helix::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
