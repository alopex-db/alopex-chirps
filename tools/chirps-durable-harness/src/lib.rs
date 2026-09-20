//! Verification-only harness for Chirps Durable evidence.
//!
//! This crate is intentionally staged outside the workspace. Task 6.0.1 owns
//! workspace activation and lockfile resolution.

/// Identifies the staged verification harness without exposing it as a package.
pub const HARNESS_NAME: &str = "chirps-fault-oracle";

#[cfg(test)]
mod tests {
    use super::HARNESS_NAME;

    #[test]
    fn identifies_the_fault_oracle() {
        assert_eq!(HARNESS_NAME, "chirps-fault-oracle");
    }
}
