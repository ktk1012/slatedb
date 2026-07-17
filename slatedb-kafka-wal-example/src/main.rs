use anyhow::{anyhow, bail, ensure, Context, Result};
use aws_msk_iam_sasl_signer::generate_auth_token;
use aws_types::region::Region;
use bytes::Bytes;
use log::{LevelFilter, Log, Metadata, Record};
use object_store::{aws::AmazonS3Builder, ObjectStore};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::error::KafkaError;
use slatedb::config::{DbReaderOptions, PutOptions, WriteOptions};
use slatedb::wal::kafka::{KafkaOAuthTokenProvider, KafkaWal, DEFAULT_COMMIT_INTERVAL};
use slatedb::{Db, DbReader, DbReaderBuilder, DbReaderMode, DbStatus};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{interval_at, sleep_until, timeout, Instant, MissedTickBehavior};
use ulid::Ulid;

const AWS_REGION: &str = "us-west-2";
const S3_BUCKET: &str = "opendata-vector-bench";
const S3_PREFIX: &str = "wal-rfc-proto";
const DEFAULT_TEST_DURATION_SECONDS: u64 = 60;
const DEFAULT_TOPIC_PREFIX: &str = "slatedb-kafka-wal-example";
const OBJECT_STORE_WAL_POLL_INTERVAL: Duration = Duration::from_millis(10);
const WRITE_INTERVAL: Duration = Duration::from_millis(1);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const STATUS_TIMEOUT: Duration = Duration::from_secs(60);
const OAUTH_TOKEN_TIMEOUT: Duration = Duration::from_secs(10);
const ADMIN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_IN_FLIGHT_WRITES: usize = 4_096;
const KEY_SIZE: usize = 32;
const VALUE_SIZE: usize = 512;

struct StderrLogger;

impl Log for StderrLogger {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            eprintln!(
                "{} [{}]: {}",
                record.level(),
                record.target(),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}

static STDERR_LOGGER: StderrLogger = StderrLogger;

fn init_logging() {
    let level = if env::var_os("KAFKA_DEBUG").is_some() {
        LevelFilter::Debug
    } else {
        LevelFilter::Warn
    };
    if log::set_logger(&STDERR_LOGGER).is_ok() {
        log::set_max_level(level);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalBackend {
    Kafka,
    ObjectStore,
}

impl WalBackend {
    fn from_env() -> Result<Self> {
        match env::var("WAL_BACKEND") {
            Ok(value) if value == "kafka" => Ok(Self::Kafka),
            Ok(value) if value == "object_store" => Ok(Self::ObjectStore),
            Ok(value) => bail!("unsupported WAL_BACKEND {value:?}; expected kafka or object_store"),
            Err(env::VarError::NotPresent) => Ok(Self::Kafka),
            Err(error) => Err(error).context("failed to read WAL_BACKEND"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Kafka => "kafka",
            Self::ObjectStore => "object_store",
        }
    }
}

#[derive(Debug)]
struct KafkaBenchmarkConfig {
    bootstrap_servers: String,
    client_properties: HashMap<String, String>,
    commit_interval: Duration,
    properties_path: PathBuf,
    topic: String,
}

impl KafkaBenchmarkConfig {
    fn from_env(run_id: &str) -> Result<Self> {
        let properties_path = env::var_os("KAFKA_CLIENT_PROPERTIES")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("client.properties"));
        let mut client_properties = read_properties(&properties_path)?;
        let bootstrap_servers = env::var("KAFKA_BOOTSTRAP_SERVERS")
            .ok()
            .or_else(|| client_properties.remove("bootstrap.servers"))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "KAFKA_BOOTSTRAP_SERVERS is required because {} has no bootstrap.servers entry",
                    properties_path.display()
                )
            })?;
        let commit_interval_ms = env_u64(
            "KAFKA_COMMIT_INTERVAL_MS",
            DEFAULT_COMMIT_INTERVAL.as_millis() as u64,
        )?;
        ensure!(
            commit_interval_ms > 0,
            "KAFKA_COMMIT_INTERVAL_MS must be greater than zero"
        );

        let topic_prefix =
            env::var("KAFKA_TOPIC_PREFIX").unwrap_or_else(|_| DEFAULT_TOPIC_PREFIX.to_string());
        ensure!(
            !topic_prefix.is_empty()
                && topic_prefix
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')),
            "KAFKA_TOPIC_PREFIX must use only ASCII letters, digits, '.', '_', and '-'"
        );
        let topic = format!("{topic_prefix}-{run_id}");
        ensure!(
            topic.len() <= 249,
            "generated Kafka topic exceeds 249 bytes"
        );

        Ok(Self {
            bootstrap_servers,
            client_properties,
            commit_interval: Duration::from_millis(commit_interval_ms),
            properties_path,
            topic,
        })
    }

    fn client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        for (key, value) in &self.client_properties {
            match key.as_str() {
                // These settings name Java classes and cannot be consumed by librdkafka.
                "sasl.jaas.config"
                | "sasl.client.callback.handler.class"
                | "sasl.login.callback.handler.class"
                | "sasl.mechanism" => {}
                _ => {
                    config.set(key, value);
                }
            }
        }
        config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanism", "OAUTHBEARER");
        if let Ok(debug) = env::var("KAFKA_DEBUG") {
            config.set("debug", debug);
            config.set_log_level(RDKafkaLogLevel::Debug);
        }
        config
    }
}

#[derive(Debug)]
struct BenchmarkConfig {
    backend: WalBackend,
    duration: Duration,
    run_id: String,
    db_path: String,
    kafka: Option<KafkaBenchmarkConfig>,
}

impl BenchmarkConfig {
    fn from_env() -> Result<Self> {
        let backend = WalBackend::from_env()?;
        let duration_seconds = env_u64("TEST_DURATION_SECONDS", DEFAULT_TEST_DURATION_SECONDS)?;
        ensure!(
            duration_seconds > 0,
            "TEST_DURATION_SECONDS must be greater than zero"
        );
        let run_id = Ulid::new().to_string().to_ascii_lowercase();
        let db_path = format!("{S3_PREFIX}/{run_id}");
        let kafka = match backend {
            WalBackend::Kafka => Some(KafkaBenchmarkConfig::from_env(&run_id)?),
            WalBackend::ObjectStore => None,
        };

        Ok(Self {
            backend,
            duration: Duration::from_secs(duration_seconds),
            run_id,
            db_path,
            kafka,
        })
    }
}

#[derive(Clone)]
struct MskOAuthProvider {
    region: Region,
    runtime: tokio::runtime::Handle,
}

impl MskOAuthProvider {
    fn new(region: &'static str) -> Self {
        Self {
            region: Region::new(region),
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl KafkaOAuthTokenProvider for MskOAuthProvider {
    fn generate_oauth_token(&self) -> std::result::Result<OAuthToken, Box<dyn Error>> {
        let debug = env::var_os("KAFKA_DEBUG").is_some();
        if debug {
            eprintln!("MSK IAM token generation started");
        }
        let region = self.region.clone();
        let runtime = self.runtime.clone();
        let generated = thread::spawn(move || {
            runtime
                .block_on(async { timeout(OAUTH_TOKEN_TIMEOUT, generate_auth_token(region)).await })
        })
        .join()
        .map_err(|_| io::Error::other("MSK OAuth token thread panicked"))?;
        let (token, lifetime_ms) = generated.map_err(|error| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("MSK OAuth token generation timed out: {error}"),
            )
        })??;

        if debug {
            eprintln!("MSK IAM token generation succeeded");
        }
        Ok(OAuthToken {
            token,
            principal_name: String::new(),
            lifetime_ms,
        })
    }
}

#[derive(Clone)]
struct AdminContext {
    oauth_provider: Arc<MskOAuthProvider>,
    debug: bool,
}

impl ClientContext for AdminContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> std::result::Result<OAuthToken, Box<dyn Error>> {
        KafkaOAuthTokenProvider::generate_oauth_token(self.oauth_provider.as_ref())
    }

    fn error(&self, error: KafkaError, reason: &str) {
        eprintln!("librdkafka error: {error}: {reason}");
    }

    fn log(&self, level: RDKafkaLogLevel, facility: &str, message: &str) {
        if self.debug {
            eprintln!("librdkafka {level:?} [{facility}]: {message}");
        }
    }
}

#[derive(Clone, Debug)]
struct WrittenRecord {
    seq: u64,
    key: Bytes,
    value: Bytes,
}

struct BenchmarkOutcome {
    writes: u64,
    elapsed: Duration,
    latencies: Vec<Duration>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let config = BenchmarkConfig::from_env()?;
    print_configuration(&config);

    let object_store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(S3_BUCKET)
            .with_region(AWS_REGION)
            .build()
            .context("failed to configure the S3 object store")?,
    );
    let (db, reader) = match config.backend {
        WalBackend::Kafka => {
            let kafka = config
                .kafka
                .as_ref()
                .context("Kafka backend configuration is missing")?;
            open_kafka_backend(&config.db_path, object_store, kafka).await?
        }
        WalBackend::ObjectStore => open_object_store_backend(&config.db_path, object_store).await?,
    };

    let outcome = run_benchmark(db.clone(), &reader, config.duration).await;
    if let Ok(outcome) = &outcome {
        if !outcome.latencies.is_empty() {
            print_results(outcome);
        }
    }

    let reader_close = reader.close().await.context("failed to close the reader");
    let db_close = db.close().await.context("failed to close the writer");
    let outcome = outcome?;
    reader_close?;
    db_close?;

    if let Some(kafka) = &config.kafka {
        println!("Kafka topic retained: {}", kafka.topic);
    }
    println!("S3 data retained: s3://{S3_BUCKET}/{}", config.db_path);
    ensure!(
        !outcome.latencies.is_empty(),
        "benchmark produced no samples"
    );
    Ok(())
}

async fn open_kafka_backend(
    db_path: &str,
    object_store: Arc<dyn ObjectStore>,
    config: &KafkaBenchmarkConfig,
) -> Result<(Db, DbReader)> {
    let oauth_provider = Arc::new(MskOAuthProvider::new(AWS_REGION));
    let admin_token = KafkaOAuthTokenProvider::generate_oauth_token(oauth_provider.as_ref())
        .map_err(|error| anyhow!("MSK IAM token preflight failed: {error}"))?;
    println!("MSK IAM token preflight succeeded");
    let kafka_client_config = config.client_config();
    create_topic(
        &kafka_client_config,
        oauth_provider.clone(),
        admin_token,
        &config.topic,
    )
    .await?;

    let kafka_wal = KafkaWal::from_configs_with_oauth_provider(
        config.topic.clone(),
        config.topic.clone(),
        kafka_client_config.clone(),
        kafka_client_config,
        config.commit_interval,
        oauth_provider,
    );
    let db = slatedb::DbBuilder::new(db_path, object_store.clone())
        .with_wal_writer(Box::new(kafka_wal.clone()))
        .build()
        .await
        .context("failed to open the SlateDB writer with the Kafka WAL")?;
    let reader = DbReaderBuilder::new(db_path, object_store)
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_wal_reader(Box::new(kafka_wal.reader()))
        .with_options(DbReaderOptions {
            tail_wal: true,
            ..DbReaderOptions::default()
        })
        .build()
        .await
        .context("failed to open the tailing SlateDB reader with the Kafka WAL")?;
    Ok((db, reader))
}

async fn open_object_store_backend(
    db_path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(Db, DbReader)> {
    let db = slatedb::DbBuilder::new(db_path, object_store.clone())
        .build()
        .await
        .context("failed to open the SlateDB writer with the object-store WAL")?;
    let reader = DbReaderBuilder::new(db_path, object_store)
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_options(DbReaderOptions {
            manifest_poll_interval: OBJECT_STORE_WAL_POLL_INTERVAL,
            tail_wal: true,
            ..DbReaderOptions::default()
        })
        .build()
        .await
        .context("failed to open the tailing SlateDB reader with the object-store WAL")?;
    Ok((db, reader))
}

async fn create_topic(
    config: &ClientConfig,
    oauth_provider: Arc<MskOAuthProvider>,
    oauth_token: OAuthToken,
    topic: &str,
) -> Result<()> {
    let admin: AdminClient<AdminContext> = config
        .create_with_context(AdminContext {
            oauth_provider,
            debug: env::var_os("KAFKA_DEBUG").is_some(),
        })
        .context("failed to create the Kafka admin client")?;
    install_admin_oauth_token(&admin, oauth_token)?;

    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(-1));
    let options = AdminOptions::new()
        .operation_timeout(Some(ADMIN_TIMEOUT))
        .request_timeout(Some(ADMIN_TIMEOUT));
    let mut results = timeout(ADMIN_TIMEOUT, admin.create_topics(&[new_topic], &options))
        .await
        .context("timed out creating the Kafka topic")?
        .context("Kafka topic creation request failed")?;
    ensure!(
        results.len() == 1,
        "Kafka returned no topic creation result"
    );
    match results.pop().expect("topic result length was checked") {
        Ok(_) => {
            println!("Created one-partition Kafka topic: {topic}");
            Ok(())
        }
        Err((name, error)) => bail!("failed to create Kafka topic {name}: {error:?}"),
    }
}

fn install_admin_oauth_token(admin: &AdminClient<AdminContext>, token: OAuthToken) -> Result<()> {
    let token_value = CString::new(token.token).context("MSK token contains a null byte")?;
    let principal_name =
        CString::new(token.principal_name).context("MSK principal contains a null byte")?;
    let mut error_buffer = [0_u8; 512];
    // SAFETY: all pointers remain valid for the duration of this call and the
    // error buffer has the length passed to librdkafka.
    let code = unsafe {
        rdkafka::bindings::rd_kafka_oauthbearer_set_token(
            admin.inner().native_ptr(),
            token_value.as_ptr(),
            token.lifetime_ms,
            principal_name.as_ptr(),
            ptr::null_mut(),
            0,
            error_buffer.as_mut_ptr().cast(),
            error_buffer.len(),
        )
    };
    if code != rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
        // SAFETY: librdkafka null-terminates the supplied error buffer.
        let message = unsafe { CStr::from_ptr(error_buffer.as_ptr().cast()) }.to_string_lossy();
        bail!("failed to install MSK OAuth token on admin client ({code:?}): {message}");
    }
    Ok(())
}

async fn run_benchmark(db: Db, reader: &DbReader, duration: Duration) -> Result<BenchmarkOutcome> {
    let started_at = Instant::now();
    let deadline = started_at + duration;
    let (latest_tx, latest_rx) = watch::channel(None);
    let writer = tokio::spawn(write_records(db.clone(), latest_tx, deadline));

    let latency_result = sample_reader_latency(&db, reader, latest_rx, deadline).await;
    let writes = writer
        .await
        .context("writer task panicked")?
        .context("writer task failed")?;
    let latencies = latency_result?;

    Ok(BenchmarkOutcome {
        writes,
        elapsed: started_at.elapsed(),
        latencies,
    })
}

async fn write_records(
    db: Db,
    latest_tx: watch::Sender<Option<WrittenRecord>>,
    deadline: Instant,
) -> Result<u64> {
    let mut ticker = interval_at(Instant::now(), WRITE_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut rng = StdRng::from_os_rng();
    let mut in_flight = JoinSet::new();
    let mut completed = 0;

    loop {
        tokio::select! {
            biased;
            _ = sleep_until(deadline) => break,
            _ = ticker.tick() => {}
        }

        let mut key = [0_u8; KEY_SIZE];
        let mut value = [0_u8; VALUE_SIZE];
        rng.fill_bytes(&mut key);
        rng.fill_bytes(&mut value);
        let key = Bytes::copy_from_slice(&key);
        let value = Bytes::copy_from_slice(&value);
        let write_db = db.clone();
        in_flight.spawn(async move {
            let handle = write_db
                .put_bytes_with_options(
                    key.clone(),
                    value.clone(),
                    &PutOptions::default(),
                    &WriteOptions {
                        await_durable: false,
                        ..WriteOptions::default()
                    },
                )
                .await
                .context("write failed")?;
            Ok::<_, anyhow::Error>(WrittenRecord {
                seq: handle.seqnum(),
                key,
                value,
            })
        });

        if in_flight.len() >= MAX_IN_FLIGHT_WRITES {
            let result = in_flight
                .join_next()
                .await
                .expect("in-flight write set is non-empty");
            record_completed_write(result, &latest_tx, &mut completed)?;
        }
        while let Some(result) = in_flight.try_join_next() {
            record_completed_write(result, &latest_tx, &mut completed)?;
        }
    }

    while let Some(result) = in_flight.join_next().await {
        record_completed_write(result, &latest_tx, &mut completed)?;
    }
    Ok(completed)
}

fn record_completed_write(
    result: std::result::Result<Result<WrittenRecord>, tokio::task::JoinError>,
    latest_tx: &watch::Sender<Option<WrittenRecord>>,
    completed: &mut u64,
) -> Result<()> {
    let record = result.context("write task panicked")??;
    latest_tx.send_if_modified(|latest| {
        if latest.as_ref().is_none_or(|latest| record.seq > latest.seq) {
            *latest = Some(record);
            true
        } else {
            false
        }
    });
    *completed += 1;
    Ok(())
}

async fn sample_reader_latency(
    db: &Db,
    reader: &DbReader,
    latest_rx: watch::Receiver<Option<WrittenRecord>>,
    deadline: Instant,
) -> Result<Vec<Duration>> {
    let mut writer_status = db.subscribe();
    let mut reader_status = reader.subscribe();
    let mut ticker = interval_at(Instant::now() + SAMPLE_INTERVAL, SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut latencies = Vec::new();
    let mut last_sampled_seq = 0;

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => break,
            _ = ticker.tick() => {}
        }

        let record = latest_rx.borrow().clone();
        let Some(record) = record else {
            continue;
        };
        if record.seq <= last_sampled_seq {
            continue;
        }
        last_sampled_seq = record.seq;

        wait_for_seq(&mut writer_status, record.seq, "writer durability").await?;
        let durable_at = Instant::now();
        wait_for_seq(&mut reader_status, record.seq, "reader visibility").await?;
        let latency = durable_at.elapsed();

        let actual = reader
            .get(&record.key)
            .await
            .with_context(|| format!("reader get failed for sequence {}", record.seq))?;
        ensure!(
            actual.as_ref() == Some(&record.value),
            "reader value mismatch at sequence {}",
            record.seq
        );
        latencies.push(latency);
    }

    Ok(latencies)
}

async fn wait_for_seq(
    status_rx: &mut watch::Receiver<DbStatus>,
    target_seq: u64,
    phase: &'static str,
) -> Result<()> {
    timeout(STATUS_TIMEOUT, async {
        loop {
            let status = status_rx.borrow_and_update().clone();
            if let Some(reason) = status.close_reason {
                bail!("database closed during {phase}: {reason:?}");
            }
            if status.durable_seq >= target_seq {
                return Ok(());
            }
            status_rx.changed().await.with_context(|| {
                format!("status channel closed during {phase} for sequence {target_seq}")
            })?;
        }
    })
    .await
    .with_context(|| format!("timed out during {phase} for sequence {target_seq}"))?
}

fn read_properties(path: &Path) -> Result<HashMap<String, String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read Kafka properties from {}", path.display()))?;
    let mut properties = HashMap::new();
    for (index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        let (key, value) = line.split_once('=').ok_or_else(|| {
            anyhow!(
                "invalid Kafka property at {}:{}; expected key=value",
                path.display(),
                index + 1
            )
        })?;
        let key = key.trim();
        ensure!(
            !key.is_empty(),
            "empty Kafka property key at {}:{}",
            path.display(),
            index + 1
        );
        properties.insert(key.to_string(), value.trim().to_string());
    }
    Ok(properties)
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be an unsigned integer")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn print_configuration(config: &BenchmarkConfig) {
    println!("SlateDB WAL benchmark");
    println!("WAL backend: {}", config.backend.as_str());
    println!("run id: {}", config.run_id);
    println!("duration: {:.0}s", config.duration.as_secs_f64());
    println!("write target: 1,000 records/s");
    println!("record size: {KEY_SIZE}-byte key, {VALUE_SIZE}-byte value");
    println!("sample interval: {}ms", SAMPLE_INTERVAL.as_millis());
    match &config.kafka {
        Some(kafka) => {
            println!(
                "Kafka WAL commit interval: {}ms",
                kafka.commit_interval.as_millis()
            );
            println!("Kafka properties: {}", kafka.properties_path.display());
            println!("Kafka topic: {}", kafka.topic);
        }
        None => println!(
            "object-store WAL poll interval: {}ms",
            OBJECT_STORE_WAL_POLL_INTERVAL.as_millis()
        ),
    }
    println!("S3 path: s3://{S3_BUCKET}/{}", config.db_path);
}

fn print_results(outcome: &BenchmarkOutcome) {
    let mut sorted = outcome.latencies.clone();
    sorted.sort_unstable();
    let average_ms =
        sorted.iter().map(Duration::as_secs_f64).sum::<f64>() * 1_000.0 / sorted.len() as f64;
    let rate = outcome.writes as f64 / outcome.elapsed.as_secs_f64();

    println!();
    println!("Results");
    println!("writes: {}", outcome.writes);
    println!("achieved write rate: {rate:.1} records/s");
    println!("verified samples: {}", sorted.len());
    println!("durable-to-readable latency:");
    println!("  average: {average_ms:.3} ms");
    println!("  p50: {:.3} ms", duration_ms(percentile(&sorted, 0.50)));
    println!("  p90: {:.3} ms", duration_ms(percentile(&sorted, 0.90)));
    println!("  p99: {:.3} ms", duration_ms(percentile(&sorted, 0.99)));
}

fn percentile(sorted: &[Duration], percentile: f64) -> Duration {
    let rank = (percentile * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
