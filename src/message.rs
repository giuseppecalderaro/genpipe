use bytes::Bytes;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// The normalized object that flows through every pipeline.
///
/// A `Message` carries a typed [`Payload`], an origin tag,
/// arbitrary key-value metadata, and a unique identifier with a timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    uuid: Uuid,
    timestamp: Timestamp,
    origin: String,
    metadata: HashMap<String, Value>,
    payload: Payload,
}

/// The data carried by a [`Message`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Payload {
    /// Structured JSON data.
    Json(Value),
    /// Plain text.
    Text(String),
    /// Raw bytes (clone is O(1) via reference counting).
    Bytes(#[serde(with = "serde_bytes_compat")] Bytes),
    /// No data — useful for signals / heartbeats.
    Empty,
}

// ── Constructors ────────────────────────────────────────────────────────

impl Message {
    /// Create a new message with the given origin and payload.
    pub fn new(origin: impl Into<String>, payload: Payload) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            timestamp: Timestamp::now(),
            origin: origin.into(),
            metadata: HashMap::new(),
            payload,
        }
    }

    /// Shorthand: create a message carrying a JSON [`Value`].
    pub fn json(origin: impl Into<String>, data: Value) -> Self {
        Self::new(origin, Payload::Json(data))
    }

    /// Shorthand: create a message carrying plain text.
    pub fn text(origin: impl Into<String>, text: impl Into<String>) -> Self {
        Self::new(origin, Payload::Text(text.into()))
    }

    /// Shorthand: create a message carrying raw bytes.
    pub fn bytes(origin: impl Into<String>, bytes: impl Into<Bytes>) -> Self {
        Self::new(origin, Payload::Bytes(bytes.into()))
    }

    /// Shorthand: create an empty signal / heartbeat message.
    pub fn empty(origin: impl Into<String>) -> Self {
        Self::new(origin, Payload::Empty)
    }

    // ── Getters ─────────────────────────────────────────────────────────

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    pub fn metadata(&self) -> &HashMap<String, Value> {
        &self.metadata
    }

    // ── Setters / builders ──────────────────────────────────────────────

    /// Fluent builder — attach a metadata entry and return `self`.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn set_metadata(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.metadata.insert(key.into(), value.into());
    }

    pub fn get_metadata(&self, key: &str) -> Option<&Value> {
        self.metadata.get(key)
    }

    pub fn set_payload(&mut self, payload: Payload) {
        self.payload = payload;
    }

    pub fn set_origin(&mut self, origin: impl Into<String>) {
        self.origin = origin.into();
    }

    pub fn into_payload(self) -> Payload {
        self.payload
    }

    // ── Serialization ───────────────────────────────────────────────────

    /// Serialize this message to JSON bytes.
    pub fn to_json(&self) -> crate::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    /// Serialize this message to a pretty-printed JSON string.
    pub fn to_json_pretty(&self) -> crate::Result<String> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }

    /// Deserialize a message from JSON bytes.
    pub fn from_json(data: &[u8]) -> crate::Result<Self> {
        serde_json::from_slice(data).map_err(Into::into)
    }

    /// Serialize this message to MessagePack bytes.
    pub fn to_msgpack(&self) -> crate::Result<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(Into::into)
    }

    /// Serialize this message to MessagePack using named keys
    /// (more readable, slightly larger than integer-keyed).
    pub fn to_msgpack_named(&self) -> crate::Result<Vec<u8>> {
        rmp_serde::to_vec_named(self).map_err(Into::into)
    }

    /// Deserialize a message from MessagePack bytes.
    pub fn from_msgpack(data: &[u8]) -> crate::Result<Self> {
        rmp_serde::from_slice(data).map_err(Into::into)
    }

    /// Serialize using the specified [`Encoding`].
    pub fn encode(&self, encoding: Encoding) -> crate::Result<Vec<u8>> {
        match encoding {
            Encoding::Json => self.to_json(),
            Encoding::MsgPack => self.to_msgpack(),
            Encoding::MsgPackNamed => self.to_msgpack_named(),
        }
    }

    /// Deserialize using the specified [`Encoding`].
    pub fn decode(data: &[u8], encoding: Encoding) -> crate::Result<Self> {
        match encoding {
            Encoding::Json => Self::from_json(data),
            Encoding::MsgPack | Encoding::MsgPackNamed => Self::from_msgpack(data),
        }
    }
}

// ── Encoding ────────────────────────────────────────────────────────────

/// Selects the wire format for [`Message`] serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    /// JSON (text).
    #[default]
    Json,
    /// MessagePack with integer keys (compact).
    MsgPack,
    /// MessagePack with string keys (more readable in tools).
    MsgPackNamed,
}

impl std::fmt::Display for Encoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Encoding::Json => write!(f, "json"),
            Encoding::MsgPack => write!(f, "msgpack"),
            Encoding::MsgPackNamed => write!(f, "msgpack_named"),
        }
    }
}

impl std::str::FromStr for Encoding {
    type Err = crate::Error;

    fn from_str(s: &str) -> crate::Result<Self> {
        match s {
            "json" => Ok(Self::Json),
            "msgpack" | "messagepack" => Ok(Self::MsgPack),
            "msgpack_named" | "messagepack_named" => Ok(Self::MsgPackNamed),
            other => Err(crate::Error::Config(format!(
                "unknown encoding `{other}`; expected json, msgpack, or msgpack_named"
            ))),
        }
    }
}

// ── Payload helpers ─────────────────────────────────────────────────────

impl Payload {
    pub fn as_json(&self) -> Option<&Value> {
        match self {
            Payload::Json(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Payload::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Payload::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Payload::Empty)
    }
}

/// Serde helper to serialize/deserialize `bytes::Bytes` as raw byte sequences.
mod serde_bytes_compat {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Bytes, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Bytes, D::Error> {
        let v: Vec<u8> = Deserialize::deserialize(de)?;
        Ok(Bytes::from(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_message() -> Message {
        Message::json("test-source", json!({"key": "value", "n": 42}))
            .with_metadata("env", "test")
    }

    #[test]
    fn json_roundtrip() {
        let msg = sample_message();
        let bytes = msg.to_json().unwrap();
        let decoded = Message::from_json(&bytes).unwrap();
        assert_eq!(decoded.origin(), "test-source");
        assert_eq!(decoded.get_metadata("env").unwrap(), "test");
        assert_eq!(
            decoded.payload().as_json().unwrap(),
            &json!({"key": "value", "n": 42})
        );
    }

    #[test]
    fn msgpack_roundtrip() {
        let msg = sample_message();
        let bytes = msg.to_msgpack().unwrap();
        let decoded = Message::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.origin(), "test-source");
        assert_eq!(
            decoded.payload().as_json().unwrap(),
            &json!({"key": "value", "n": 42})
        );
    }

    #[test]
    fn msgpack_named_roundtrip() {
        let msg = sample_message();
        let bytes = msg.to_msgpack_named().unwrap();
        let decoded = Message::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.origin(), "test-source");
    }

    #[test]
    fn msgpack_is_smaller_than_json() {
        let msg = sample_message();
        let json_bytes = msg.to_json().unwrap();
        let mp_bytes = msg.to_msgpack().unwrap();
        assert!(
            mp_bytes.len() < json_bytes.len(),
            "msgpack ({}) should be smaller than json ({})",
            mp_bytes.len(),
            json_bytes.len()
        );
    }

    #[test]
    fn encode_decode_with_encoding_enum() {
        let msg = sample_message();
        for enc in [Encoding::Json, Encoding::MsgPack, Encoding::MsgPackNamed] {
            let bytes = msg.encode(enc).unwrap();
            let decoded = Message::decode(&bytes, enc).unwrap();
            assert_eq!(decoded.origin(), msg.origin());
        }
    }

    #[test]
    fn encoding_from_str() {
        assert_eq!("json".parse::<Encoding>().unwrap(), Encoding::Json);
        assert_eq!("msgpack".parse::<Encoding>().unwrap(), Encoding::MsgPack);
        assert_eq!("messagepack".parse::<Encoding>().unwrap(), Encoding::MsgPack);
        assert_eq!(
            "msgpack_named".parse::<Encoding>().unwrap(),
            Encoding::MsgPackNamed
        );
        assert!("xml".parse::<Encoding>().is_err());
    }

    #[test]
    fn all_payload_variants_roundtrip_msgpack() {
        let cases = vec![
            Message::text("s", "hello world"),
            Message::bytes("s", vec![0xDE, 0xAD, 0xBE, 0xEF]),
            Message::empty("s"),
            Message::json("s", json!({"nested": [1, 2, 3]})),
        ];
        for msg in cases {
            let bytes = msg.to_msgpack().unwrap();
            let decoded = Message::from_msgpack(&bytes).unwrap();
            assert_eq!(format!("{:?}", decoded.payload()), format!("{:?}", msg.payload()));
        }
    }
}

