use async_trait::async_trait;

use super::{Processor, ProcessorOutput};
use crate::{Message, Result};

/// Transforms each message using a synchronous closure.
///
/// ```rust,ignore
/// use helix::components::processors::MapProcessor;
///
/// let upper = MapProcessor::new("uppercase", |mut msg| {
///     if let Some(text) = msg.payload().as_text() {
///         msg.set_payload(helix::Payload::Text(text.to_uppercase()));
///     }
///     msg
/// });
/// ```
pub struct MapProcessor<F>
where
    F: Fn(Message) -> Message + Send + Sync,
{
    name: String,
    func: F,
}

impl<F> MapProcessor<F>
where
    F: Fn(Message) -> Message + Send + Sync,
{
    pub fn new(name: impl Into<String>, func: F) -> Self {
        Self {
            name: name.into(),
            func,
        }
    }
}

#[async_trait]
impl<F> Processor for MapProcessor<F>
where
    F: Fn(Message) -> Message + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn process(&self, message: Message) -> Result<ProcessorOutput> {
        Ok(ProcessorOutput::Single((self.func)(message)))
    }
}

/// Drops messages that do not match a predicate.
///
/// ```rust,ignore
/// use helix::components::processors::FilterProcessor;
///
/// let only_text = FilterProcessor::new("text-only", |msg| {
///     msg.payload().as_text().is_some()
/// });
/// ```
pub struct FilterProcessor<F>
where
    F: Fn(&Message) -> bool + Send + Sync,
{
    name: String,
    predicate: F,
}

impl<F> FilterProcessor<F>
where
    F: Fn(&Message) -> bool + Send + Sync,
{
    pub fn new(name: impl Into<String>, predicate: F) -> Self {
        Self {
            name: name.into(),
            predicate,
        }
    }
}

#[async_trait]
impl<F> Processor for FilterProcessor<F>
where
    F: Fn(&Message) -> bool + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn process(&self, message: Message) -> Result<ProcessorOutput> {
        if (self.predicate)(&message) {
            Ok(ProcessorOutput::Single(message))
        } else {
            Ok(ProcessorOutput::Drop)
        }
    }
}

/// A processor powered by an arbitrary async closure returning
/// [`ProcessorOutput`].
///
/// ```rust,ignore
/// use helix::components::processors::FnProcessor;
/// use helix::ProcessorOutput;
///
/// let enricher = FnProcessor::new("enrich", |msg| Box::pin(async move {
///     // … async enrichment logic …
///     Ok(ProcessorOutput::Single(msg))
/// }));
/// ```
pub struct FnProcessor<F>
where
    F: Fn(Message) -> crate::BoxFuture<'static, Result<ProcessorOutput>> + Send + Sync,
{
    name: String,
    func: F,
}

impl<F> FnProcessor<F>
where
    F: Fn(Message) -> crate::BoxFuture<'static, Result<ProcessorOutput>> + Send + Sync,
{
    pub fn new(name: impl Into<String>, func: F) -> Self {
        Self {
            name: name.into(),
            func,
        }
    }
}

#[async_trait]
impl<F> Processor for FnProcessor<F>
where
    F: Fn(Message) -> crate::BoxFuture<'static, Result<ProcessorOutput>> + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn process(&self, message: Message) -> Result<ProcessorOutput> {
        (self.func)(message).await
    }
}
