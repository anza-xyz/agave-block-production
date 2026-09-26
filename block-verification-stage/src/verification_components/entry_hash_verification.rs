#[cfg(feature = "dev-context-only-utils")]
use mockall::automock;

#[cfg_attr(feature = "dev-context-only-utils", automock)]
pub trait VerifyEntryHash {}
