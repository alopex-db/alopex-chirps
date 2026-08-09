use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
struct Packet {
    index: u32,
    attempt: u8,
    data: Vec<u8>,
}

#[derive(Debug)]
struct Ack {
    index: u32,
    ok: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let drop_index = std::env::var("POC_NACK_INDEX")
        .ok()
        .and_then(|v| v.parse::<u32>().ok());
    let (tx, mut rx) = mpsc::channel::<Packet>(2);
    let (ack_tx, mut ack_rx) = mpsc::channel::<Ack>(2);
    let sink = tokio::spawn(async move {
        let mut seen = HashSet::new();
        while let Some(packet) = rx.recv().await {
            let _attempt = packet.attempt;
            let _payload_len = packet.data.len();
            let _duplicate = !seen.insert(packet.index);
            ack_tx
                .send(Ack {
                    index: packet.index,
                    ok: true,
                })
                .await?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(seen)
    });

    let mut retries = 0u32;
    let mut delivered = 0u32;
    for index in 0..16u32 {
        let mut attempt = 0u8;
        loop {
            attempt += 1;
            if attempt > 3 {
                return Err(format!("retry limit exceeded for {index}").into());
            }
            tx.send(Packet {
                index,
                attempt,
                data: vec![index as u8; 1024],
            })
            .await?;
            let ack = tokio::time::timeout(Duration::from_secs(1), ack_rx.recv())
                .await?
                .ok_or("ack channel closed")?;
            let force_nack = drop_index == Some(index) && attempt == 1;
            if ack.index == index && ack.ok && !force_nack {
                delivered += 1;
                break;
            }
            retries += 1;
        }
    }
    drop(tx);
    let seen = sink.await??;
    assert_eq!(seen.len(), 16);
    assert_eq!(delivered, 16);
    println!(
        "ack_retry_poc delivered={delivered} unique_seen={} retries={retries} nack_index={drop_index:?}",
        seen.len()
    );
    Ok(())
}
