use std::fmt;

use anyhow::Result;

use crate::ActorConfig;
use crate::actor::lifecycle_hooks::ActorStart;
use crate::runtime::RuntimeBoxFuture;

#[cfg(feature = "wasm-runtime")]
pub type ActorEntryFn = dyn Fn(ActorStart) -> RuntimeBoxFuture<Result<()>>;

#[cfg(not(feature = "wasm-runtime"))]
pub type ActorEntryFn = dyn Fn(ActorStart) -> RuntimeBoxFuture<Result<()>> + Send + Sync;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorActionMode {
	Concurrent,
	Transactional,
}

#[cfg(feature = "wasm-runtime")]
type ActorActionClassifier = dyn Fn(&str, &[u8]) -> ActorActionMode;

#[cfg(not(feature = "wasm-runtime"))]
type ActorActionClassifier = dyn Fn(&str, &[u8]) -> ActorActionMode + Send + Sync;

/// Runtime extension point for building actor receive loops.
pub struct ActorFactory {
	config: ActorConfig,
	entry: Box<ActorEntryFn>,
	manual_startup_ready: bool,
	streams_request_body: bool,
	transactional_operations: bool,
	action_classifier: Option<Box<ActorActionClassifier>>,
}

#[cfg(feature = "wasm-runtime")]
unsafe impl Send for ActorFactory {}

#[cfg(feature = "wasm-runtime")]
unsafe impl Sync for ActorFactory {}

impl ActorFactory {
	pub fn new<F>(config: ActorConfig, entry: F) -> Self
	where
		F: ActorEntry,
	{
		Self {
			config,
			entry: Box::new(entry),
			manual_startup_ready: false,
			streams_request_body: false,
			transactional_operations: false,
			action_classifier: None,
		}
	}

	/// Builds a factory whose runtime will explicitly signal `startup_ready`
	/// after its own startup preamble finishes.
	pub fn new_with_manual_startup_ready<F>(config: ActorConfig, entry: F) -> Self
	where
		F: ActorEntry,
	{
		Self {
			config,
			entry: Box::new(entry),
			manual_startup_ready: true,
			streams_request_body: false,
			transactional_operations: false,
			action_classifier: None,
		}
	}

	/// Declares that this runtime consumes request bodies incrementally.
	pub fn with_streaming_request_body(mut self) -> Self {
		self.streams_request_body = true;
		self
	}

	/// Declares that this runtime durably commits each operation before replying.
	pub fn with_transactional_operations(mut self) -> Self {
		self.transactional_operations = true;
		self
	}

	/// Classifies which actions consume the transactional dispatch slot.
	///
	/// Concurrent actions must be independently enforced as read-only by the runtime.
	#[cfg(not(feature = "wasm-runtime"))]
	pub fn with_action_classifier<F>(mut self, classifier: F) -> Self
	where
		F: Fn(&str, &[u8]) -> ActorActionMode + Send + Sync + 'static,
	{
		self.action_classifier = Some(Box::new(classifier));
		self
	}

	/// Classifies which actions consume the transactional dispatch slot.
	#[cfg(feature = "wasm-runtime")]
	pub fn with_action_classifier<F>(mut self, classifier: F) -> Self
	where
		F: Fn(&str, &[u8]) -> ActorActionMode + 'static,
	{
		self.action_classifier = Some(Box::new(classifier));
		self
	}

	pub fn config(&self) -> &ActorConfig {
		&self.config
	}

	pub(crate) fn requires_manual_startup_ready(&self) -> bool {
		self.manual_startup_ready
	}

	pub(crate) fn streams_request_body(&self) -> bool {
		self.streams_request_body
	}

	pub(crate) fn supports_transactional_operations(&self) -> bool {
		self.transactional_operations
	}

	pub(crate) fn action_mode(&self, name: &str, args: &[u8]) -> ActorActionMode {
		self.action_classifier
			.as_ref()
			.map(|classifier| classifier(name, args))
			.unwrap_or(ActorActionMode::Transactional)
	}

	pub async fn start(&self, start: ActorStart) -> Result<()> {
		(self.entry)(start).await
	}
}

#[cfg(feature = "wasm-runtime")]
pub trait ActorEntry: Fn(ActorStart) -> RuntimeBoxFuture<Result<()>> + 'static {}

#[cfg(feature = "wasm-runtime")]
impl<F> ActorEntry for F where F: Fn(ActorStart) -> RuntimeBoxFuture<Result<()>> + 'static {}

#[cfg(not(feature = "wasm-runtime"))]
pub trait ActorEntry:
	Fn(ActorStart) -> RuntimeBoxFuture<Result<()>> + Send + Sync + 'static
{
}

#[cfg(not(feature = "wasm-runtime"))]
impl<F> ActorEntry for F where
	F: Fn(ActorStart) -> RuntimeBoxFuture<Result<()>> + Send + Sync + 'static
{
}

impl fmt::Debug for ActorFactory {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorFactory")
			.field("config", &self.config)
			.field("manual_startup_ready", &self.manual_startup_ready)
			.field("streams_request_body", &self.streams_request_body)
			.field("transactional_operations", &self.transactional_operations)
			.field(
				"action_classifier",
				&self
					.action_classifier
					.as_ref()
					.map(|_| "<boxed classifier>"),
			)
			.field("entry", &"<boxed entry>")
			.finish()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_factory() -> ActorFactory {
		ActorFactory::new(ActorConfig::default(), |_start| Box::pin(async { Ok(()) }))
	}

	#[test]
	fn action_classifier_defaults_to_transactional_and_can_mark_queries_concurrent() {
		let factory = test_factory();
		assert_eq!(
			factory.action_mode("read", b"query"),
			ActorActionMode::Transactional
		);

		let factory = test_factory().with_action_classifier(|name, _args| {
			if name == "read" {
				ActorActionMode::Concurrent
			} else {
				ActorActionMode::Transactional
			}
		});
		assert_eq!(
			factory.action_mode("read", b"query"),
			ActorActionMode::Concurrent
		);
		assert_eq!(
			factory.action_mode("write", b"command"),
			ActorActionMode::Transactional
		);
	}
}
