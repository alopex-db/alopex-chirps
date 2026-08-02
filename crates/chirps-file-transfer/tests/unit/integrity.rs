use alopex_chirps_file_transfer::{HashAlgorithm, IntegrityVerifier};
use sha2::Digest;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use xxhash_rust::xxh64::Xxh64;

#[test]
fn chunk_checksum_matches_expected() {
    let data = b"hello chunk";
    let checksum = IntegrityVerifier::compute_chunk_checksum(data);
    assert!(IntegrityVerifier::verify_chunk_checksum(data, checksum));

    let wrong = checksum.wrapping_add(1);
    assert!(!IntegrityVerifier::verify_chunk_checksum(data, wrong));
}

#[tokio::test]
async fn file_hashes_match_reference() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("data.txt");
    let data = b"hash me";
    let mut file = File::create(&path).await.expect("create file");
    file.write_all(data).await.expect("write file");
    file.flush().await.expect("flush file");

    let sha_hash = IntegrityVerifier::compute_file_hash(&path, HashAlgorithm::Sha256)
        .await
        .expect("sha256 hash");
    let mut sha = sha2::Sha256::new();
    sha.update(data);
    assert_eq!(sha_hash, sha.finalize().to_vec());

    let blake_hash = IntegrityVerifier::compute_file_hash(&path, HashAlgorithm::Blake3)
        .await
        .expect("blake3 hash");
    let mut blake = blake3::Hasher::new();
    blake.update(data);
    assert_eq!(blake_hash, blake.finalize().as_bytes().to_vec());

    let xx_hash = IntegrityVerifier::compute_file_hash(&path, HashAlgorithm::XxHash64)
        .await
        .expect("xxhash hash");
    let mut xx = Xxh64::new(0);
    xx.update(data);
    assert_eq!(xx_hash, xx.digest().to_be_bytes().to_vec());
}

#[tokio::test]
async fn file_hash_and_chunk_metadata_share_one_scan() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("chunks.bin");
    let data: Vec<u8> = (0..70_000).map(|index| (index % 251) as u8).collect();
    let mut file = File::create(&path).await.expect("create file");
    file.write_all(&data).await.expect("write file");
    file.flush().await.expect("flush file");

    let (hash, chunks) = IntegrityVerifier::compute_file_hash_and_chunk_metas(
        &path,
        HashAlgorithm::Sha256,
        16 * 1024,
    )
    .await
    .expect("single scan");

    assert_eq!(
        hash,
        IntegrityVerifier::compute_file_hash(&path, HashAlgorithm::Sha256)
            .await
            .expect("reference hash")
    );
    assert_eq!(chunks.len(), 5);
    assert_eq!(chunks[0].offset, 0);
    assert_eq!(chunks[4].size, 4_464);
    for chunk in &chunks {
        let start = chunk.offset as usize;
        let end = start + chunk.size as usize;
        assert_eq!(
            chunk.checksum,
            IntegrityVerifier::compute_chunk_checksum(&data[start..end])
        );
    }
}
