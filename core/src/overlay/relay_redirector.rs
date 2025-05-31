//! Stream relay implementation
//!
//! This module has been refactored into a submodule structure for better maintainability.
//! See the `relay/` directory for the implementation details.
//!
//! This file now serves as a re-export layer for backward compatibility.

// Re-export from our new module structure
pub use crate::overlay::relay::*;
