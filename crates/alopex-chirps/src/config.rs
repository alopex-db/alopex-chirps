pub use alopex_chirps_core::config::*;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_config() {
        let config = NodeConfig::default();
        assert_eq!(config.bind_addr, "127.0.0.1:0".parse().unwrap());
        assert_eq!(config.ping_timeout, std::time::Duration::from_secs(1));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation() {
        // Both provided and exist
        let cert_file = NamedTempFile::new().unwrap();
        let key_file = NamedTempFile::new().unwrap();
        let config = NodeConfig {
            cert_path: Some(cert_file.path().to_path_buf()),
            key_path: Some(key_file.path().to_path_buf()),
            ..Default::default()
        };
        assert!(config.validate().is_ok());

        // Cert missing
        let config = NodeConfig {
            cert_path: Some(std::path::PathBuf::from("missing.crt")),
            key_path: Some(key_file.path().to_path_buf()),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Key missing
        let config = NodeConfig {
            cert_path: Some(cert_file.path().to_path_buf()),
            key_path: Some(std::path::PathBuf::from("missing.key")),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
