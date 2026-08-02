use alopex_chirps_file_transfer::{ChunkManager, IntegrityVerifier};
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn chunk_manager_reads_chunks_and_metas() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("data.bin");

    let data_size = 70 * 1024;
    let mut data = Vec::with_capacity(data_size);
    for i in 0..data_size {
        data.push((i % 256) as u8);
    }

    let mut file = File::create(&path).await.expect("create file");
    file.write_all(&data).await.expect("write file");
    file.flush().await.expect("flush file");

    let manager = ChunkManager::new(32 * 1024);
    let mut file = File::open(&path).await.expect("open file");
    let metas = manager
        .generate_chunk_metas(&mut file, data.len() as u64)
        .await
        .expect("generate metas");

    assert_eq!(metas.len(), 2);
    assert_eq!(metas[0].index, 0);
    assert_eq!(metas[0].offset, 0);
    assert_eq!(metas[0].size as usize, manager.chunk_size());
    assert_eq!(metas[1].index, 1);
    assert_eq!(metas[1].offset as usize, manager.chunk_size());
    assert_eq!(metas[1].size as usize, data.len() - manager.chunk_size());

    let mut file = File::open(&path).await.expect("open file for chunk");
    let chunk0 = manager.read_chunk(&mut file, 0).await.expect("read chunk0");
    assert_eq!(chunk0.data.len(), manager.chunk_size());
    assert_eq!(chunk0.data, data[..manager.chunk_size()]);
    assert!(IntegrityVerifier::verify_chunk_checksum(
        &chunk0.data,
        chunk0.checksum
    ));

    let mut file = File::open(&path).await.expect("open file for chunk");
    let chunk1 = manager.read_chunk(&mut file, 1).await.expect("read chunk1");
    assert_eq!(
        chunk1.data,
        data[manager.chunk_size()..manager.chunk_size() + chunk1.data.len()]
    );
    assert!(IntegrityVerifier::verify_chunk_checksum(
        &chunk1.data,
        chunk1.checksum
    ));
}
