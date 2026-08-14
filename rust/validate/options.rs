use crate::limits::Limits;

/// The amount of an Acta file validation decodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationLevel {
    /// Validate framing, schema metadata, and data-frame metadata only.
    Structural,
    /// Decode every complete data block and verify its logical invariants.
    Full,
}

/// Options controlling Acta validation.
///
/// The fields are private so new validation controls can be added without
/// exposing an unvalidated configuration structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationOptions {
    level: ValidationLevel,
    limits: Limits,
}

impl ValidationOptions {
    /// Return these options with the requested validation level.
    pub fn with_level(mut self, level: ValidationLevel) -> Self {
        self.level = level;
        self
    }

    /// Return these options with caller-supplied resource limits.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// The selected validation level.
    pub fn level(&self) -> ValidationLevel {
        self.level
    }

    /// The resource limits applied during validation.
    pub fn limits(&self) -> Limits {
        self.limits
    }
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self {
            level: ValidationLevel::Structural,
            limits: Limits::default(),
        }
    }
}
