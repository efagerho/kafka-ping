use lazy_static::lazy_static;
use protobuf::Message as ProtoMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message as KafkaMessage;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::signal;

use hdrhistogram::Histogram;

mod protos;
use crate::protos::payload::{Payload, Ping};

lazy_static! {
    static ref BOOTSTRAP: String =
        env::var("KAFKA_BOOTSTRAP").unwrap_or("localhost:9092".to_string());
    static ref TOPIC: String = env::var("KAFKA_TOPIC").unwrap_or("kafka-ping".to_string());
    static ref USERNAME: String = env::var("KAFKA_USERNAME").unwrap_or("".to_string());
    static ref PASSWORD: String = env::var("KAFKA_PASSWORD").unwrap_or("".to_string());
    static ref CONSUMER_GROUP_ID: String =
        env::var("KAFKA_CONSUMER_GROUP_ID").unwrap_or("kafka-ping".to_string());
}

static COUNTER: AtomicUsize = AtomicUsize::new(0);
static BANDWIDTH: AtomicUsize = AtomicUsize::new(0);
static DONE: AtomicBool = AtomicBool::new(false);

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

//
// Kafka helpers
//

fn create_producer() -> FutureProducer {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", BOOTSTRAP.clone());
    config.set("message.timeout.ms", "5000");

    if !USERNAME.is_empty() {
        config.set("security.protocol", "SASL_SSL");
        config.set("sasl.mechanism", "SCRAM-SHA-512");
        config.set("sasl.jaas.config", scram_auth_string(&USERNAME, &PASSWORD));
    }

    config.create().expect("Error creating producer")
}

fn create_consumer() -> StreamConsumer {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", BOOTSTRAP.clone());
    //config.set("session.timeout.ms", "50000");
    config.set("enable.auto.commit", "false");
    config.set("group.id", CONSUMER_GROUP_ID.clone());

    if !USERNAME.is_empty() {
        println!("Using SCRAM auth for consumer");
        config.set("security.protocol", "SASL_SSL");
        config.set("sasl.mechanism", "SCRAM-SHA-512");
        config.set("sasl.jaas.config", scram_auth_string(&USERNAME, &PASSWORD));
    }

    config.create().expect("Error creating producer")
}

fn scram_auth_string(username: &str, password: &str) -> String {
    format!("org.apache.kafka.common.security.scram.ScramLoginModule required \nusername=\"{}\"\npassword=\"{}\";", username, password)
}

//
// Main program
//

#[tokio::main]
async fn main() {
    let producer: FutureProducer = create_producer();
    let consumer: StreamConsumer = create_consumer();
    consumer
        .subscribe(&[&TOPIC])
        .expect("error listening to Kafka topic");

    println!("Starting producers and consumers...");

    // Print output stats every second
    let stats_task = tokio::task::spawn(async {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        while !DONE.load(Ordering::Acquire) {
            interval.tick().await;
            println!(
                "PPS {}, {} MB/s",
                COUNTER.load(Ordering::Acquire),
                BANDWIDTH.load(Ordering::Acquire)
            );
            COUNTER.store(0, Ordering::Release);
            BANDWIDTH.store(0, Ordering::Release);
        }
    });

    // Producer loop
    let producer_task = tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        interval.tick().await;
        let mut i = 0_usize;
        while !DONE.load(Ordering::Acquire) {
            let ping = Ping {
                timestamp: now(),
                sender_id: "kafka-ping".to_string(),
                ..Default::default()
            };
            let mut payload = Payload::new();
            payload.set_ping(ping);

            producer
                .send_result(
                    FutureRecord::to(&TOPIC)
                        .key(&i.to_string())
                        .payload(&payload.write_to_bytes().unwrap()),
                )
                .unwrap()
                .await
                .unwrap()
                .unwrap();
            i += 1;
            COUNTER.fetch_add(1, Ordering::AcqRel);
        }
    });

    let consumer_task = tokio::task::spawn(async move {
        let start = Instant::now();
        let mut latencies = Histogram::<u64>::new(5).unwrap();

        while !DONE.load(Ordering::Acquire) {
            let message = consumer.recv().await.unwrap();
            let mut payload = Payload::new();
            payload
                .merge_from_bytes(message.payload().unwrap())
                .unwrap();

            if !payload.has_ping() {
                continue;
            }

            let then = payload.ping().timestamp;

            if start.elapsed() < Duration::from_secs(10) {
                // Warming up.
            } else {
                latencies += (now() - then) as u64;
            }
        }

        println!("measurements: {}", latencies.len());
        println!("mean latency: {}ms", latencies.mean());
        println!("p50 latency:  {}ms", latencies.value_at_quantile(0.50));
        println!("p90 latency:  {}ms", latencies.value_at_quantile(0.90));
        println!("p99 latency:  {}ms", latencies.value_at_quantile(0.99));
        println!("p99.9 latency:  {}ms", latencies.value_at_quantile(0.999));
    });

    signal::ctrl_c().await.expect("failed to listen for CTRL-C");
    DONE.store(true, Ordering::Release);

    stats_task.await.unwrap();
    producer_task.await.unwrap();
    consumer_task.await.unwrap();
}
