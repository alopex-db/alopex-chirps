//! Adapter-owned cancellation-safe TCP/TLS transport.
//!
//! The transport deliberately does not use Iggy's high-level `TcpClient`.
//! One coordinator serializes explicit login or data-plane exchanges through
//! one writer and one reader worker. A shared close handle cancels blocked I/O,
//! closes the owned stream halves, and leaves all three workers joinable.

use crate::protocol::{
    APPEND_ONE_SYNCED_CODE, CAPABILITY_BIND_CODE, CHECKED_POLL_CODE, LEASE_RENEW_CODE,
    PrivateCommand, PrivateRequest, ResourceLocation,
};
use alopex_chirps_core::durable::DurableMessageId;
use bytes::{Bytes, BytesMut};
use iggy_binary_protocol::codes::{
    GET_TOPIC_CODE, LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, SEND_MESSAGES_CODE,
    STORE_CONSUMER_OFFSET_CODE,
};
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::requests::messages::{RawMessage, SendMessagesEncoder};
use iggy_binary_protocol::requests::topics::GetTopicRequest;
use iggy_binary_protocol::{
    RequestFrame, WireConsumer, WireEncode, WireIdentifier, WirePartitioning,
};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf, split};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};
use tokio_rustls::TlsConnector;

const OFFICIAL_RESPONSE_HEADER_LEN: usize = 8;
const COMMAND_QUEUE_CAPACITY: usize = 1;
const WORKER_COUNT: usize = 3;

/// Fixed complete-frame bound for one owned connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportLimits {
    max_frame_len: usize,
}

impl TransportLimits {
    /// Creates a bound large enough for the official response header.
    pub fn new(max_frame_len: usize) -> Result<Self, TransportError> {
        if max_frame_len < OFFICIAL_RESPONSE_HEADER_LEN {
            return Err(TransportError::InvalidFrameLimit {
                minimum: OFFICIAL_RESPONSE_HEADER_LEN,
                actual: max_frame_len,
            });
        }
        Ok(Self { max_frame_len })
    }

    fn validate_frame(self, frame: &Bytes) -> Result<(), TransportError> {
        if frame.is_empty() {
            return Err(TransportError::EmptyFrame);
        }
        if frame.len() > self.max_frame_len {
            return Err(TransportError::FrameTooLarge {
                actual: frame.len(),
                maximum: self.max_frame_len,
            });
        }
        Ok(())
    }
}

/// Exact transport stage that failed without exposing request data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportStage {
    /// Establishing a TCP socket.
    Connect,
    /// Performing the caller-configured TLS handshake.
    TlsHandshake,
    /// Writing one complete request frame.
    Write,
    /// Flushing the owned writer after the one request frame.
    Flush,
    /// Reading the fixed official response header.
    ReadHeader,
    /// Reading the bounded official response payload.
    ReadBody,
}

/// Sealed request class used in bounded wrong-entrypoint errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportRequestClass {
    /// Official username/password or personal-access-token login.
    Login,
    /// Private CapabilityBind or LeaseRenew on the authenticated connection.
    SessionControl,
    /// Standard count-one append or a private Durable data-plane command.
    DataPlane,
}

/// One owned worker role, used only for bounded failure reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportWorker {
    /// Exchange coordinator.
    Coordinator,
    /// Socket writer.
    Writer,
    /// Socket reader.
    Reader,
}

/// Bounded transport failure with the ambiguity classification kept separate.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransportError {
    /// The configured complete-frame bound cannot contain an official header.
    #[error("transport frame limit {actual} is below minimum {minimum}")]
    InvalidFrameLimit { minimum: usize, actual: usize },
    /// Empty bytes are not an invocation frame.
    #[error("transport frame is empty")]
    EmptyFrame,
    /// The official request frame could not be decoded exactly once.
    #[error("transport request is not one complete official request frame")]
    InvalidRequestFrame,
    /// Bytes followed one complete request frame.
    #[error("transport request contains trailing or concatenated frame bytes")]
    TrailingRequestBytes,
    /// A command was submitted through the wrong sealed entrypoint.
    #[error("command {actual:#010x} is not valid for {expected:?}")]
    WrongRequestClass {
        expected: TransportRequestClass,
        actual: u32,
    },
    /// A request or declared response exceeds the connection's fixed bound.
    #[error("transport frame length {actual} exceeds maximum {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    /// The owned connection has been closed.
    #[error("owned transport is closed")]
    Closed,
    /// A worker stopped before returning the requested stage result.
    #[error("owned transport {0:?} worker stopped")]
    WorkerStopped(TransportWorker),
    /// An operating-system or TLS I/O error at a bounded stage.
    #[error("transport I/O failed at {stage:?}: {kind:?}")]
    Io {
        stage: TransportStage,
        kind: io::ErrorKind,
    },
}

impl TransportError {
    fn io(stage: TransportStage, error: &io::Error) -> Self {
        Self::Io {
            stage,
            kind: error.kind(),
        }
    }
}

/// One exactly decoded official login request frame.
#[derive(Clone, PartialEq, Eq)]
pub struct LoginRequestFrame {
    bytes: Bytes,
}

impl std::fmt::Debug for LoginRequestFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LoginRequestFrame")
            .field("encoded_len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl TryFrom<Bytes> for LoginRequestFrame {
    type Error = TransportError;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let code = decode_one_official_request(&bytes)?;
        if !matches!(
            code,
            LOGIN_USER_CODE | LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
        ) {
            return Err(TransportError::WrongRequestClass {
                expected: TransportRequestClass::Login,
                actual: code,
            });
        }
        Ok(Self { bytes })
    }
}

/// One canonical private CapabilityBind or LeaseRenew request frame.
#[derive(Clone, PartialEq, Eq)]
pub struct SessionControlRequestFrame {
    bytes: Bytes,
}

impl std::fmt::Debug for SessionControlRequestFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionControlRequestFrame")
            .field("encoded_len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl SessionControlRequestFrame {
    /// Encodes one approved private session-control request.
    pub fn from_private(request: &PrivateRequest) -> Result<Self, TransportError> {
        if !matches!(
            request.command(),
            PrivateCommand::CapabilityBind | PrivateCommand::LeaseRenew
        ) {
            return Err(TransportError::WrongRequestClass {
                expected: TransportRequestClass::SessionControl,
                actual: request.command().code(),
            });
        }
        let bytes = request
            .encode()
            .map_err(|_| TransportError::InvalidRequestFrame)?;
        let code = decode_one_official_request(&bytes)?;
        debug_assert!(matches!(code, CAPABILITY_BIND_CODE | LEASE_RENEW_CODE));
        Ok(Self { bytes })
    }
}

/// One official authenticated `GetTopic` request used only to verify the
/// development profile before any append attempt can start.
#[derive(Clone, PartialEq, Eq)]
pub struct DevelopmentProfileReadbackFrame {
    bytes: Bytes,
}

impl std::fmt::Debug for DevelopmentProfileReadbackFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DevelopmentProfileReadbackFrame")
            .field("encoded_len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl DevelopmentProfileReadbackFrame {
    /// Encodes an exact numeric stream/topic resource readback request.
    pub fn for_location(location: ResourceLocation) -> Result<Self, TransportError> {
        let request = GetTopicRequest {
            stream_id: WireIdentifier::numeric(location.stream_id()),
            topic_id: WireIdentifier::numeric(location.topic_id()),
        };
        let payload = request.to_bytes();
        let mut frame = BytesMut::with_capacity(payload.len().saturating_add(8));
        RequestFrame::encode(GET_TOPIC_CODE, &payload, &mut frame)
            .map_err(|_| TransportError::InvalidRequestFrame)?;
        let bytes = frame.freeze();
        let code = decode_one_official_request(&bytes)?;
        debug_assert_eq!(code, GET_TOPIC_CODE);
        Ok(Self { bytes })
    }
}

/// One canonical official or private data-plane request frame.
///
/// There is intentionally no arbitrary-byte constructor. The future official
/// development append path must add its own count-one typed encoder rather
/// than weaken this sealed boundary.
#[derive(Clone, PartialEq, Eq)]
pub struct DataPlaneRequestFrame {
    bytes: Bytes,
}

impl std::fmt::Debug for DataPlaneRequestFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DataPlaneRequestFrame")
            .field("encoded_len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl DataPlaneRequestFrame {
    /// Encodes one official 0.10.0 `SendMessages` command with a numeric
    /// stream/topic, explicit partition, count one, and exactly one immutable
    /// canonical envelope. No high-level producer, batching, or dedup policy is
    /// involved.
    pub fn from_standard_append(
        location: ResourceLocation,
        message_id: DurableMessageId,
        canonical_envelope: &[u8],
    ) -> Result<Self, TransportError> {
        if canonical_envelope.is_empty() {
            return Err(TransportError::InvalidRequestFrame);
        }
        let stream_id = WireIdentifier::numeric(location.stream_id());
        let topic_id = WireIdentifier::numeric(location.topic_id());
        let partitioning = WirePartitioning::PartitionId(location.partition_id());
        let messages = [RawMessage {
            id: u128::from_be_bytes(*message_id.as_bytes()),
            origin_timestamp: 0,
            headers: None,
            payload: canonical_envelope,
        }];
        let payload_len =
            SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &messages);
        let mut payload = BytesMut::with_capacity(payload_len);
        SendMessagesEncoder::encode(
            &mut payload,
            &stream_id,
            &topic_id,
            &partitioning,
            &messages,
        );
        let mut frame = BytesMut::with_capacity(payload.len().saturating_add(8));
        RequestFrame::encode(SEND_MESSAGES_CODE, &payload, &mut frame)
            .map_err(|_| TransportError::InvalidRequestFrame)?;
        let bytes = frame.freeze();
        let code = decode_one_official_request(&bytes)?;
        debug_assert_eq!(code, SEND_MESSAGES_CODE);
        Ok(Self { bytes })
    }

    /// Encodes one official 0.10.0 `StoreConsumerOffset` command for an
    /// ordinary numeric consumer, numeric stream/topic, explicit partition,
    /// and one already committed inclusive offset. Consumer groups and an
    /// absent partition cannot be constructed through this entrypoint.
    pub fn from_standard_store_consumer_offset(
        consumer_id: u32,
        location: ResourceLocation,
        committed_offset: u64,
    ) -> Result<Self, TransportError> {
        let request = StoreConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(consumer_id)),
            stream_id: WireIdentifier::numeric(location.stream_id()),
            topic_id: WireIdentifier::numeric(location.topic_id()),
            partition_id: Some(location.partition_id()),
            offset: committed_offset,
        };
        let payload = request.to_bytes();
        let mut frame = BytesMut::with_capacity(payload.len().saturating_add(8));
        RequestFrame::encode(STORE_CONSUMER_OFFSET_CODE, &payload, &mut frame)
            .map_err(|_| TransportError::InvalidRequestFrame)?;
        let bytes = frame.freeze();
        let code = decode_one_official_request(&bytes)?;
        debug_assert_eq!(code, STORE_CONSUMER_OFFSET_CODE);
        Ok(Self { bytes })
    }

    /// Encodes one approved private data-plane request.
    pub fn from_private(request: &PrivateRequest) -> Result<Self, TransportError> {
        if !matches!(
            request.command(),
            PrivateCommand::AppendOneSynced | PrivateCommand::CheckedPoll
        ) {
            return Err(TransportError::WrongRequestClass {
                expected: TransportRequestClass::DataPlane,
                actual: request.command().code(),
            });
        }
        let bytes = request
            .encode()
            .map_err(|_| TransportError::InvalidRequestFrame)?;
        let code = decode_one_official_request(&bytes)?;
        debug_assert!(matches!(code, APPEND_ONE_SYNCED_CODE | CHECKED_POLL_CODE));
        Ok(Self { bytes })
    }
}

fn decode_one_official_request(bytes: &[u8]) -> Result<u32, TransportError> {
    if bytes.is_empty() {
        return Err(TransportError::EmptyFrame);
    }
    let (frame, consumed) =
        RequestFrame::decode(bytes).map_err(|_| TransportError::InvalidRequestFrame)?;
    if consumed != bytes.len() {
        return Err(TransportError::TrailingRequestBytes);
    }
    Ok(frame.code)
}

/// Public append ambiguity boundary for one data-plane invocation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvocationError {
    /// Local validation or a proven-prior close prevented invocation.
    #[error("data-plane command was not invoked: {0}")]
    NotInvoked(TransportError),
    /// Invocation was queued exactly once but no valid response was confirmed.
    #[error("data-plane command outcome is indeterminate: {0}")]
    Indeterminate(TransportError),
}

/// Idempotent control that wakes both blocked I/O workers and the coordinator.
#[derive(Debug, Clone)]
pub struct TransportCloseHandle {
    close_tx: watch::Sender<bool>,
}

impl TransportCloseHandle {
    /// Requests permanent socket close. Repeated calls have no extra effect.
    pub fn close(&self) {
        self.close_tx.send_replace(true);
    }

    /// Returns whether close has already been requested.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        *self.close_tx.borrow()
    }
}

/// Terminal evidence that every owned task stopped before shutdown returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportShutdownReport {
    socket_close_requested: bool,
    joined_workers: usize,
    forced_abort_count: usize,
    panicked_worker_count: usize,
    data_plane_invocations: u64,
}

impl TransportShutdownReport {
    /// Returns whether the socket-close signal was issued.
    #[must_use]
    pub const fn socket_close_requested(self) -> bool {
        self.socket_close_requested
    }

    /// Returns true only after all owned coordinator/read/write tasks joined.
    #[must_use]
    pub const fn all_workers_joined(self) -> bool {
        self.joined_workers == WORKER_COUNT
    }

    /// Returns tasks forcibly aborted only after the supplied deadline elapsed.
    #[must_use]
    pub const fn forced_abort_count(self) -> usize {
        self.forced_abort_count
    }

    /// Returns workers that panicked while being joined.
    #[must_use]
    pub const fn panicked_worker_count(self) -> usize {
        self.panicked_worker_count
    }

    /// Returns the number of data-plane jobs admitted by the coordinator.
    #[must_use]
    pub const fn data_plane_invocations(self) -> u64 {
        self.data_plane_invocations
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExchangeKind {
    Login,
    SessionControl,
    DataPlane,
}

struct ExchangeJob {
    kind: ExchangeKind,
    request: Bytes,
    reply: oneshot::Sender<Result<Bytes, TransportError>>,
}

struct WriteJob {
    request: Bytes,
    reply: oneshot::Sender<Result<(), TransportError>>,
}

struct ReadJob {
    reply: oneshot::Sender<Result<Bytes, TransportError>>,
}

struct TransportTasks {
    coordinator: JoinHandle<()>,
    writer: JoinHandle<()>,
    reader: JoinHandle<()>,
}

/// One non-reconnecting owned connection with explicit login and invocation.
pub struct OwnedTransport {
    limits: TransportLimits,
    exchange_tx: Option<mpsc::Sender<ExchangeJob>>,
    close: TransportCloseHandle,
    tasks: Option<TransportTasks>,
    data_plane_invocations: Arc<AtomicU64>,
}

impl std::fmt::Debug for OwnedTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedTransport")
            .field("limits", &self.limits)
            .field("closed", &self.close.is_closed())
            .field(
                "data_plane_invocations",
                &self.data_plane_invocations.load(Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

impl OwnedTransport {
    /// Connects one plain TCP socket without login, retry, or replay.
    pub async fn connect_tcp(
        address: SocketAddr,
        limits: TransportLimits,
    ) -> Result<Self, TransportError> {
        let stream = TcpStream::connect(address)
            .await
            .map_err(|error| TransportError::io(TransportStage::Connect, &error))?;
        stream
            .set_nodelay(true)
            .map_err(|error| TransportError::io(TransportStage::Connect, &error))?;
        Ok(Self::from_io(stream, limits))
    }

    /// Connects one caller-configured TLS socket without auto-login or retry.
    pub async fn connect_tls(
        address: SocketAddr,
        server_name: ServerName<'static>,
        client_config: Arc<ClientConfig>,
        limits: TransportLimits,
    ) -> Result<Self, TransportError> {
        let stream = TcpStream::connect(address)
            .await
            .map_err(|error| TransportError::io(TransportStage::Connect, &error))?;
        stream
            .set_nodelay(true)
            .map_err(|error| TransportError::io(TransportStage::Connect, &error))?;
        let tls_stream = TlsConnector::from(client_config)
            .connect(server_name, stream)
            .await
            .map_err(|error| TransportError::io(TransportStage::TlsHandshake, &error))?;
        Ok(Self::from_io(tls_stream, limits))
    }

    // Private deterministic seam. Production construction is restricted to
    // the direct TCP/TLS constructors above.
    fn from_io<S>(io: S, limits: TransportLimits) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (reader, writer) = split(io);
        let (close_tx, close_rx) = watch::channel(false);
        let close = TransportCloseHandle { close_tx };
        let (exchange_tx, exchange_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (write_tx, write_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (read_tx, read_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let data_plane_invocations = Arc::new(AtomicU64::new(0));

        let writer_task = tokio::spawn(writer_worker(writer, write_rx, close_rx.clone()));
        let reader_task = tokio::spawn(reader_worker(reader, read_rx, close_rx.clone(), limits));
        let coordinator_task = tokio::spawn(coordinator_worker(
            exchange_rx,
            write_tx,
            read_tx,
            close_rx,
            close.close_tx.clone(),
            Arc::clone(&data_plane_invocations),
        ));

        Self {
            limits,
            exchange_tx: Some(exchange_tx),
            close,
            tasks: Some(TransportTasks {
                coordinator: coordinator_task,
                writer: writer_task,
                reader: reader_task,
            }),
            data_plane_invocations,
        }
    }

    /// Returns a cloneable socket-close control for deadline coordination.
    #[must_use]
    pub fn close_handle(&self) -> TransportCloseHandle {
        self.close.clone()
    }

    /// Writes exactly one explicit login frame and reads exactly one response.
    /// No authentication data is synthesized or logged by this layer.
    pub async fn login(&self, request: LoginRequestFrame) -> Result<Bytes, TransportError> {
        self.limits.validate_frame(&request.bytes)?;
        if self.close.is_closed() {
            return Err(TransportError::Closed);
        }
        self.exchange(ExchangeKind::Login, request.bytes).await
    }

    /// Writes exactly one typed CapabilityBind or LeaseRenew frame without
    /// counting it as a data-plane invocation.
    pub async fn session_control(
        &self,
        request: SessionControlRequestFrame,
    ) -> Result<Bytes, TransportError> {
        self.limits.validate_frame(&request.bytes)?;
        if self.close.is_closed() {
            return Err(TransportError::Closed);
        }
        self.exchange(ExchangeKind::SessionControl, request.bytes)
            .await
    }

    /// Performs one authenticated official metadata readback without counting
    /// it as an append/data-plane invocation.
    pub async fn development_profile_readback(
        &self,
        request: DevelopmentProfileReadbackFrame,
    ) -> Result<Bytes, TransportError> {
        self.limits.validate_frame(&request.bytes)?;
        if self.close.is_closed() {
            return Err(TransportError::Closed);
        }
        self.exchange(ExchangeKind::SessionControl, request.bytes)
            .await
    }

    /// Invokes exactly one data-plane frame. There is no retry, replay,
    /// reconnect, batching, or automatic login. Once queued, every failure is
    /// indeterminate because no valid response was confirmed.
    pub async fn invoke(&self, request: DataPlaneRequestFrame) -> Result<Bytes, InvocationError> {
        self.invoke_with_admission(request, || true).await
    }

    /// Invokes one data-plane frame after `on_admitted` authorizes the exact
    /// transition into the owned coordinator queue. Rejection proves that no
    /// future automatic append was queued.
    pub async fn invoke_with_admission<F>(
        &self,
        request: DataPlaneRequestFrame,
        on_admitted: F,
    ) -> Result<Bytes, InvocationError>
    where
        F: FnOnce() -> bool + Send,
    {
        self.limits
            .validate_frame(&request.bytes)
            .map_err(InvocationError::NotInvoked)?;
        if self.close.is_closed() {
            return Err(InvocationError::NotInvoked(TransportError::Closed));
        }
        if !on_admitted() {
            return Err(InvocationError::NotInvoked(TransportError::Closed));
        }
        self.exchange(ExchangeKind::DataPlane, request.bytes)
            .await
            .map_err(InvocationError::Indeterminate)
    }

    async fn exchange(&self, kind: ExchangeKind, request: Bytes) -> Result<Bytes, TransportError> {
        let Some(exchange_tx) = &self.exchange_tx else {
            return Err(TransportError::Closed);
        };
        let (reply, response) = oneshot::channel();
        let job = ExchangeJob {
            kind,
            request,
            reply,
        };
        let mut close_rx = self.close.close_tx.subscribe();
        tokio::select! {
            biased;
            () = wait_until_closed(&mut close_rx) => return Err(TransportError::Closed),
            result = exchange_tx.send(job) => {
                result.map_err(|_| TransportError::WorkerStopped(TransportWorker::Coordinator))?;
            }
        }
        tokio::select! {
            biased;
            result = response => result
                .map_err(|_| TransportError::WorkerStopped(TransportWorker::Coordinator))?,
            () = wait_until_closed(&mut close_rx) => Err(TransportError::Closed),
        }
    }

    /// Permanently closes the socket and joins coordinator/read/write workers.
    /// If the supplied deadline is already exhausted, remaining workers are
    /// aborted and still awaited before this method returns.
    pub async fn shutdown(mut self, deadline: Instant) -> TransportShutdownReport {
        self.close.close();
        self.exchange_tx.take();
        let mut joined_workers = 0;
        let mut forced_abort_count = 0;
        let mut panicked_worker_count = 0;
        if let Some(tasks) = self.tasks.take() {
            for handle in [tasks.coordinator, tasks.writer, tasks.reader] {
                let outcome = join_worker(handle, deadline).await;
                joined_workers += 1;
                forced_abort_count += usize::from(outcome.forced_abort);
                panicked_worker_count += usize::from(outcome.panicked);
            }
        }
        TransportShutdownReport {
            socket_close_requested: self.close.is_closed(),
            joined_workers,
            forced_abort_count,
            panicked_worker_count,
            data_plane_invocations: self.data_plane_invocations.load(Ordering::Acquire),
        }
    }
}

impl Drop for OwnedTransport {
    fn drop(&mut self) {
        self.close.close();
        self.exchange_tx.take();
        if let Some(tasks) = self.tasks.take() {
            tasks.coordinator.abort();
            tasks.writer.abort();
            tasks.reader.abort();
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct JoinOutcome {
    forced_abort: bool,
    panicked: bool,
}

async fn join_worker(mut handle: JoinHandle<()>, deadline: Instant) -> JoinOutcome {
    match timeout_at(deadline, &mut handle).await {
        Ok(result) => JoinOutcome {
            forced_abort: false,
            panicked: result.is_err(),
        },
        Err(_) => {
            handle.abort();
            let result = handle.await;
            JoinOutcome {
                forced_abort: true,
                panicked: result.is_err() && !result.is_err_and(|error| error.is_cancelled()),
            }
        }
    }
}

async fn coordinator_worker(
    mut exchange_rx: mpsc::Receiver<ExchangeJob>,
    write_tx: mpsc::Sender<WriteJob>,
    read_tx: mpsc::Sender<ReadJob>,
    mut close_rx: watch::Receiver<bool>,
    close_tx: watch::Sender<bool>,
    data_plane_invocations: Arc<AtomicU64>,
) {
    loop {
        let job = tokio::select! {
            biased;
            () = wait_until_closed(&mut close_rx) => break,
            job = exchange_rx.recv() => match job {
                Some(job) => job,
                None => break,
            },
        };
        if job.kind == ExchangeKind::DataPlane {
            data_plane_invocations.fetch_add(1, Ordering::AcqRel);
        }
        let result = perform_exchange(&write_tx, &read_tx, &mut close_rx, job.request).await;
        let close_after_reply = result.is_err();
        let _ = job.reply.send(result);
        if close_after_reply {
            close_tx.send_replace(true);
            break;
        }
    }
}

async fn perform_exchange(
    write_tx: &mpsc::Sender<WriteJob>,
    read_tx: &mpsc::Sender<ReadJob>,
    close_rx: &mut watch::Receiver<bool>,
    request: Bytes,
) -> Result<Bytes, TransportError> {
    let (write_reply, write_result) = oneshot::channel();
    let write_job = WriteJob {
        request,
        reply: write_reply,
    };
    tokio::select! {
        biased;
        () = wait_until_closed(close_rx) => return Err(TransportError::Closed),
        result = write_tx.send(write_job) => {
            result.map_err(|_| TransportError::WorkerStopped(TransportWorker::Writer))?;
        }
    }
    tokio::select! {
        biased;
        () = wait_until_closed(close_rx) => return Err(TransportError::Closed),
        result = write_result => {
            result
                .map_err(|_| TransportError::WorkerStopped(TransportWorker::Writer))??;
        }
    }

    let (read_reply, read_result) = oneshot::channel();
    tokio::select! {
        biased;
        () = wait_until_closed(close_rx) => return Err(TransportError::Closed),
        result = read_tx.send(ReadJob { reply: read_reply }) => {
            result.map_err(|_| TransportError::WorkerStopped(TransportWorker::Reader))?;
        }
    }
    tokio::select! {
        biased;
        () = wait_until_closed(close_rx) => Err(TransportError::Closed),
        result = read_result => result
            .map_err(|_| TransportError::WorkerStopped(TransportWorker::Reader))?,
    }
}

async fn writer_worker<W>(
    mut writer: WriteHalf<W>,
    mut write_rx: mpsc::Receiver<WriteJob>,
    mut close_rx: watch::Receiver<bool>,
) where
    W: AsyncWrite + Unpin,
{
    loop {
        let job = tokio::select! {
            biased;
            () = wait_until_closed(&mut close_rx) => break,
            job = write_rx.recv() => match job {
                Some(job) => job,
                None => break,
            },
        };
        let result = write_one(&mut writer, &mut close_rx, &job.request).await;
        let closed = matches!(result, Err(TransportError::Closed));
        let _ = job.reply.send(result);
        if closed {
            break;
        }
    }
    let _ = writer.shutdown().await;
}

async fn write_one<W>(
    writer: &mut W,
    close_rx: &mut watch::Receiver<bool>,
    request: &[u8],
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    tokio::select! {
        biased;
        () = wait_until_closed(close_rx) => return Err(TransportError::Closed),
        result = writer.write_all(request) => {
            result.map_err(|error| TransportError::io(TransportStage::Write, &error))?;
        }
    }
    tokio::select! {
        biased;
        () = wait_until_closed(close_rx) => Err(TransportError::Closed),
        result = writer.flush() => result
            .map_err(|error| TransportError::io(TransportStage::Flush, &error)),
    }
}

async fn reader_worker<R>(
    mut reader: ReadHalf<R>,
    mut read_rx: mpsc::Receiver<ReadJob>,
    mut close_rx: watch::Receiver<bool>,
    limits: TransportLimits,
) where
    R: AsyncRead + Unpin,
{
    loop {
        let job = tokio::select! {
            biased;
            () = wait_until_closed(&mut close_rx) => break,
            job = read_rx.recv() => match job {
                Some(job) => job,
                None => break,
            },
        };
        let result = read_one(&mut reader, &mut close_rx, limits).await;
        let closed = matches!(result, Err(TransportError::Closed));
        let _ = job.reply.send(result);
        if closed {
            break;
        }
    }
}

async fn read_one<R>(
    reader: &mut R,
    close_rx: &mut watch::Receiver<bool>,
    limits: TransportLimits,
) -> Result<Bytes, TransportError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0_u8; OFFICIAL_RESPONSE_HEADER_LEN];
    tokio::select! {
        biased;
        () = wait_until_closed(close_rx) => return Err(TransportError::Closed),
        result = reader.read_exact(&mut header) => {
            result.map_err(|error| TransportError::io(TransportStage::ReadHeader, &error))?;
        }
    }
    let payload_len = u32::from_le_bytes(header[4..].try_into().expect("fixed response header"));
    let complete_len = OFFICIAL_RESPONSE_HEADER_LEN
        .checked_add(payload_len as usize)
        .ok_or(TransportError::FrameTooLarge {
            actual: usize::MAX,
            maximum: limits.max_frame_len,
        })?;
    if complete_len > limits.max_frame_len {
        return Err(TransportError::FrameTooLarge {
            actual: complete_len,
            maximum: limits.max_frame_len,
        });
    }
    let mut frame = BytesMut::zeroed(complete_len);
    frame[..OFFICIAL_RESPONSE_HEADER_LEN].copy_from_slice(&header);
    if payload_len != 0 {
        tokio::select! {
            biased;
            () = wait_until_closed(close_rx) => return Err(TransportError::Closed),
            result = reader.read_exact(&mut frame[OFFICIAL_RESPONSE_HEADER_LEN..]) => {
                result.map_err(|error| TransportError::io(TransportStage::ReadBody, &error))?;
            }
        }
    }
    Ok(frame.freeze())
}

async fn wait_until_closed(close_rx: &mut watch::Receiver<bool>) {
    if *close_rx.borrow() {
        return;
    }
    loop {
        if close_rx.changed().await.is_err() || *close_rx.borrow() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DataPlaneRequestFrame, InvocationError, LoginRequestFrame, OwnedTransport,
        SessionControlRequestFrame, TransportError, TransportLimits, TransportRequestClass,
        TransportStage,
    };
    use crate::protocol::{
        CAPABILITY_BIND_CODE, CapabilityBindRequest, PrivateRequest, ResourceLocation,
    };
    use alopex_chirps_core::durable::{DurableMessageId, ResourceId};
    use bytes::{Bytes, BytesMut};
    use iggy_binary_protocol::codes::{
        LOGIN_USER_CODE, SEND_MESSAGES_CODE, STORE_CONSUMER_OFFSET_CODE,
    };
    use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
    use iggy_binary_protocol::requests::messages::SendMessagesHeader;
    use iggy_binary_protocol::{
        RequestFrame, ResponseFrame, WireDecode, WireIdentifier, WirePartitioning,
    };
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{Notify, oneshot};
    use tokio::time::{Duration, Instant, timeout};

    const LOGIN_CODE: u32 = LOGIN_USER_CODE;
    const APPEND_CODE: u32 = 0x8000_0703;

    fn request(code: u32, payload: &[u8]) -> Bytes {
        let mut frame = BytesMut::new();
        RequestFrame::encode(code, payload, &mut frame).unwrap();
        frame.freeze()
    }

    fn response(payload: &[u8]) -> Bytes {
        let mut frame = BytesMut::new();
        ResponseFrame::encode_ok(payload, &mut frame).unwrap();
        frame.freeze()
    }

    fn login_request(payload: &[u8]) -> LoginRequestFrame {
        LoginRequestFrame::try_from(request(LOGIN_CODE, payload)).unwrap()
    }

    fn data_request(payload: &[u8]) -> DataPlaneRequestFrame {
        DataPlaneRequestFrame {
            bytes: request(APPEND_CODE, payload),
        }
    }

    fn session_request() -> SessionControlRequestFrame {
        let request =
            PrivateRequest::CapabilityBind(CapabilityBindRequest::new(1, 2, 0, 30_000).unwrap());
        SessionControlRequestFrame::from_private(&request).unwrap()
    }

    #[test]
    fn v07_task_3_7_standard_append_frame_is_count_one_and_explicit_partition() {
        let mut resource_id = [0x17; 16];
        resource_id[6] = 0x47;
        resource_id[8] = 0x97;
        let location =
            ResourceLocation::new(ResourceId::from_bytes(resource_id), 3, 11, 22, 4).unwrap();
        let message_id = DurableMessageId::generate().unwrap();
        let canonical = b"one canonical envelope";

        let request =
            DataPlaneRequestFrame::from_standard_append(location, message_id, canonical).unwrap();
        let (frame, consumed) = RequestFrame::decode(&request.bytes).unwrap();

        assert_eq!(consumed, request.bytes.len());
        assert_eq!(frame.code, SEND_MESSAGES_CODE);
        let metadata_len = u32::from_le_bytes(frame.payload[..4].try_into().unwrap()) as usize;
        let (header, header_len) =
            SendMessagesHeader::decode(&frame.payload[4..4 + metadata_len]).unwrap();
        assert_eq!(header_len, metadata_len);
        assert_eq!(header.stream_id, WireIdentifier::numeric(11));
        assert_eq!(header.topic_id, WireIdentifier::numeric(22));
        assert_eq!(header.partitioning, WirePartitioning::PartitionId(4));
        assert_eq!(header.messages_count, 1);

        let message_start = 4 + metadata_len + 16;
        let wire_id = u128::from_le_bytes(
            frame.payload[message_start + 8..message_start + 24]
                .try_into()
                .unwrap(),
        );
        let payload_len = u32::from_le_bytes(
            frame.payload[message_start + 52..message_start + 56]
                .try_into()
                .unwrap(),
        ) as usize;
        assert_eq!(wire_id, u128::from_be_bytes(*message_id.as_bytes()));
        assert_eq!(payload_len, canonical.len());
        assert_eq!(
            &frame.payload[message_start + 64..message_start + 64 + payload_len],
            canonical
        );
    }

    #[test]
    fn v07_task_3_8_offset_frame_is_regular_consumer_and_explicit_partition() {
        let mut resource_id = [0x27; 16];
        resource_id[6] = 0x47;
        resource_id[8] = 0x97;
        let location =
            ResourceLocation::new(ResourceId::from_bytes(resource_id), 3, 11, 22, 4).unwrap();

        let request =
            DataPlaneRequestFrame::from_standard_store_consumer_offset(77, location, 123).unwrap();
        let (frame, consumed) = RequestFrame::decode(&request.bytes).unwrap();
        assert_eq!(consumed, request.bytes.len());
        assert_eq!(frame.code, STORE_CONSUMER_OFFSET_CODE);
        let (decoded, payload_consumed) =
            StoreConsumerOffsetRequest::decode(frame.payload).unwrap();
        assert_eq!(payload_consumed, frame.payload.len());
        assert_eq!(decoded.consumer.kind, 1, "must be an ordinary consumer");
        assert_eq!(decoded.consumer.id, WireIdentifier::numeric(77));
        assert_eq!(decoded.stream_id, WireIdentifier::numeric(11));
        assert_eq!(decoded.topic_id, WireIdentifier::numeric(22));
        assert_eq!(decoded.partition_id, Some(4));
        assert_eq!(decoded.offset, 123);
    }

    async fn read_request(stream: &mut TcpStream) -> io::Result<(u32, Vec<u8>)> {
        let mut header = [0_u8; 8];
        stream.read_exact(&mut header).await?;
        let declared = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let payload_len = declared
            .checked_sub(4)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short request length"))?;
        let code = u32::from_le_bytes(header[4..].try_into().unwrap());
        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload).await?;
        Ok((code, payload))
    }

    async fn write_response(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
        stream.write_all(&response(payload)).await
    }

    #[tokio::test]
    async fn v07_task_3_5_explicit_login_and_one_data_plane_invocation_have_no_hidden_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let first = read_request(&mut stream).await.unwrap();
            write_response(&mut stream, b"login-ok").await.unwrap();
            let second = read_request(&mut stream).await.unwrap();
            write_response(&mut stream, b"bind-ok").await.unwrap();
            let third = read_request(&mut stream).await.unwrap();
            write_response(&mut stream, b"append-ok").await.unwrap();
            (first, second, third)
        });

        let transport = OwnedTransport::connect_tcp(address, TransportLimits::new(1024).unwrap())
            .await
            .unwrap();
        assert_eq!(
            transport.login(login_request(b"login")).await,
            Ok(response(b"login-ok"))
        );
        assert_eq!(
            transport.session_control(session_request()).await,
            Ok(response(b"bind-ok"))
        );
        assert_eq!(
            transport.invoke(data_request(b"one")).await,
            Ok(response(b"append-ok"))
        );
        let report = transport
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.all_workers_joined());
        assert_eq!(report.forced_abort_count(), 0);
        assert_eq!(report.data_plane_invocations(), 1);
        let (first, second, third) = server.await.unwrap();
        assert_eq!(first, (LOGIN_CODE, b"login".to_vec()));
        assert_eq!(second.0, CAPABILITY_BIND_CODE);
        assert!(!second.1.is_empty());
        assert_eq!(third, (APPEND_CODE, b"one".to_vec()));
    }

    #[test]
    fn v07_task_3_5_sealed_entrypoints_reject_concatenation_and_cross_class_commands() {
        let mut concatenated = BytesMut::new();
        concatenated.extend_from_slice(&request(LOGIN_CODE, b"first"));
        concatenated.extend_from_slice(&request(LOGIN_CODE, b"second"));
        assert_eq!(
            LoginRequestFrame::try_from(concatenated.freeze()),
            Err(TransportError::TrailingRequestBytes)
        );
        assert_eq!(
            LoginRequestFrame::try_from(request(APPEND_CODE, b"append")),
            Err(TransportError::WrongRequestClass {
                expected: TransportRequestClass::Login,
                actual: APPEND_CODE,
            })
        );

        let bind =
            PrivateRequest::CapabilityBind(CapabilityBindRequest::new(1, 2, 0, 30_000).unwrap());
        assert!(SessionControlRequestFrame::from_private(&bind).is_ok());
        assert_eq!(
            DataPlaneRequestFrame::from_private(&bind),
            Err(TransportError::WrongRequestClass {
                expected: TransportRequestClass::DataPlane,
                actual: CAPABILITY_BIND_CODE,
            })
        );

        let login = login_request(b"credential-not-for-debug");
        let data = data_request(b"payload-not-for-debug");
        assert!(!format!("{login:?}").contains("credential-not-for-debug"));
        assert!(!format!("{data:?}").contains("payload-not-for-debug"));
    }

    #[tokio::test]
    async fn v07_task_3_5_disconnect_after_invocation_is_indeterminate_and_never_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let server_accepts = Arc::clone(&accepts);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            server_accepts.fetch_add(1, Ordering::SeqCst);
            let command = read_request(&mut stream).await.unwrap();
            drop(stream);
            let second = timeout(Duration::from_millis(100), listener.accept()).await;
            (command, second.is_ok())
        });

        let transport = OwnedTransport::connect_tcp(address, TransportLimits::new(1024).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            transport.invoke(data_request(b"once")).await,
            Err(InvocationError::Indeterminate(TransportError::Io {
                stage: TransportStage::ReadHeader,
                ..
            }))
        ));
        let report = transport
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.all_workers_joined());
        let (command, reconnected) = server.await.unwrap();
        assert_eq!(command, (APPEND_CODE, b"once".to_vec()));
        assert!(!reconnected);
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn v07_task_3_5_deadline_close_releases_blocked_read_and_joins_every_worker() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (received_tx, received_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let command = read_request(&mut stream).await.unwrap();
            received_tx.send(command).unwrap();
            let mut byte = [0_u8; 1];
            stream.read(&mut byte).await.unwrap()
        });

        let transport = OwnedTransport::connect_tcp(address, TransportLimits::new(1024).unwrap())
            .await
            .unwrap();
        let close = transport.close_handle();
        let mut invocation = Box::pin(transport.invoke(data_request(b"blocked")));
        let command = tokio::select! {
            command = received_rx => command.unwrap(),
            result = &mut invocation => panic!("response unexpectedly completed: {result:?}"),
        };
        assert_eq!(command, (APPEND_CODE, b"blocked".to_vec()));
        close.close();
        assert!(matches!(
            invocation.await,
            Err(InvocationError::Indeterminate(TransportError::Closed))
        ));
        let report = transport
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.socket_close_requested());
        assert!(report.all_workers_joined());
        assert_eq!(report.forced_abort_count(), 0);
        assert_eq!(server.await.unwrap(), 0, "peer must observe EOF");
    }

    struct BlockedWriteIo {
        write_polls: Arc<AtomicUsize>,
        write_started: Arc<Notify>,
    }

    impl AsyncRead for BlockedWriteIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for BlockedWriteIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            self.write_polls.fetch_add(1, Ordering::SeqCst);
            self.write_started.notify_one();
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn v07_task_3_5_deadline_close_releases_blocked_write_without_second_invocation() {
        let write_polls = Arc::new(AtomicUsize::new(0));
        let write_started = Arc::new(Notify::new());
        let transport = OwnedTransport::from_io(
            BlockedWriteIo {
                write_polls: Arc::clone(&write_polls),
                write_started: Arc::clone(&write_started),
            },
            TransportLimits::new(1024).unwrap(),
        );
        let close = transport.close_handle();
        let mut invocation = Box::pin(transport.invoke(data_request(b"blocked-write")));
        tokio::select! {
            () = write_started.notified() => {},
            result = &mut invocation => panic!("write unexpectedly completed: {result:?}"),
        }
        close.close();
        assert!(matches!(
            invocation.await,
            Err(InvocationError::Indeterminate(TransportError::Closed))
        ));
        let report = transport
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.all_workers_joined());
        assert_eq!(report.forced_abort_count(), 0);
        assert!(write_polls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn v07_task_3_7_transport_admission_fires_once_only_after_queue_acceptance() {
        let write_polls = Arc::new(AtomicUsize::new(0));
        let write_started = Arc::new(Notify::new());
        let admissions = Arc::new(AtomicUsize::new(0));
        let transport = OwnedTransport::from_io(
            BlockedWriteIo {
                write_polls: Arc::clone(&write_polls),
                write_started: Arc::clone(&write_started),
            },
            TransportLimits::new(64).unwrap(),
        );

        let rejected_admissions = Arc::clone(&admissions);
        assert!(matches!(
            transport
                .invoke_with_admission(
                    DataPlaneRequestFrame {
                        bytes: Bytes::from(vec![0; 65]),
                    },
                    move || {
                        rejected_admissions.fetch_add(1, Ordering::SeqCst);
                        true
                    },
                )
                .await,
            Err(InvocationError::NotInvoked(
                TransportError::FrameTooLarge { .. }
            ))
        ));
        assert_eq!(admissions.load(Ordering::SeqCst), 0);

        let accepted_admissions = Arc::clone(&admissions);
        let mut invocation = Box::pin(transport.invoke_with_admission(
            data_request(b"one-admitted-append"),
            move || {
                accepted_admissions.fetch_add(1, Ordering::SeqCst);
                true
            },
        ));
        tokio::select! {
            () = write_started.notified() => {},
            result = &mut invocation => panic!("write unexpectedly completed: {result:?}"),
        }
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        transport.close_handle().close();
        assert!(matches!(
            invocation.await,
            Err(InvocationError::Indeterminate(TransportError::Closed))
        ));
        let report = transport
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert_eq!(report.data_plane_invocations(), 1);
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn v07_task_3_5_close_before_invocation_and_local_bounds_are_not_submitted() {
        let write_polls = Arc::new(AtomicUsize::new(0));
        let transport = OwnedTransport::from_io(
            BlockedWriteIo {
                write_polls: Arc::clone(&write_polls),
                write_started: Arc::new(Notify::new()),
            },
            TransportLimits::new(32).unwrap(),
        );
        assert!(matches!(
            transport
                .invoke(DataPlaneRequestFrame {
                    bytes: Bytes::from(vec![0; 33]),
                })
                .await,
            Err(InvocationError::NotInvoked(TransportError::FrameTooLarge {
                actual: 33,
                maximum: 32
            }))
        ));
        let close = transport.close_handle();
        close.close();
        assert_eq!(
            transport.invoke(data_request(b"never")).await,
            Err(InvocationError::NotInvoked(TransportError::Closed))
        );
        let report = transport
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.all_workers_joined());
        assert_eq!(write_polls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn v07_task_3_5_oversized_response_after_invocation_is_indeterminate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request(&mut stream).await.unwrap();
            stream.write_all(&0_u32.to_le_bytes()).await.unwrap();
            stream.write_all(&1024_u32.to_le_bytes()).await.unwrap();
        });
        let transport = OwnedTransport::connect_tcp(address, TransportLimits::new(64).unwrap())
            .await
            .unwrap();
        assert_eq!(
            transport.invoke(data_request(b"one")).await,
            Err(InvocationError::Indeterminate(
                TransportError::FrameTooLarge {
                    actual: 1032,
                    maximum: 64
                }
            ))
        );
        let report = transport
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.all_workers_joined());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn v07_task_3_5_caller_cancellation_never_replays_and_shutdown_joins() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (received_tx, received_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let command = read_request(&mut stream).await.unwrap();
            received_tx.send(command).unwrap();
            let mut byte = [0_u8; 1];
            stream.read(&mut byte).await.unwrap()
        });
        let transport = OwnedTransport::connect_tcp(address, TransportLimits::new(1024).unwrap())
            .await
            .unwrap();
        let mut abandoned = Box::pin(transport.invoke(data_request(b"cancelled")));
        let command = tokio::select! {
            command = received_rx => command.unwrap(),
            result = &mut abandoned => panic!("response unexpectedly completed: {result:?}"),
        };
        assert_eq!(command, (APPEND_CODE, b"cancelled".to_vec()));
        drop(abandoned);
        let report = transport
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.all_workers_joined());
        assert_eq!(report.data_plane_invocations(), 1);
        assert_eq!(server.await.unwrap(), 0);
    }

    #[tokio::test]
    async fn v07_task_3_5_cancelled_caller_is_drained_before_the_next_exchange() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (first_received_tx, first_received_rx) = oneshot::channel();
        let (release_first_tx, release_first_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let first = read_request(&mut stream).await.unwrap();
            first_received_tx.send(first).unwrap();
            release_first_rx.await.unwrap();
            write_response(&mut stream, b"abandoned-response")
                .await
                .unwrap();
            let second = read_request(&mut stream).await.unwrap();
            write_response(&mut stream, b"second-response")
                .await
                .unwrap();
            second
        });

        let transport = OwnedTransport::connect_tcp(address, TransportLimits::new(1024).unwrap())
            .await
            .unwrap();
        let mut abandoned = Box::pin(transport.invoke(data_request(b"first")));
        let first = tokio::select! {
            first = first_received_rx => first.unwrap(),
            result = &mut abandoned => panic!("first response unexpectedly completed: {result:?}"),
        };
        assert_eq!(first, (APPEND_CODE, b"first".to_vec()));
        drop(abandoned);
        release_first_tx.send(()).unwrap();
        assert_eq!(
            transport.invoke(data_request(b"second")).await,
            Ok(response(b"second-response"))
        );
        let report = transport
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.all_workers_joined());
        assert_eq!(report.data_plane_invocations(), 2);
        assert_eq!(server.await.unwrap(), (APPEND_CODE, b"second".to_vec()));
    }
}
