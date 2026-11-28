//! Reading `dora topic` data straight from zenoh (dora-rs/dora#4025).
//!
//! The coordinator answers a direct subscription with one
//! [`ZenohTopicSource`] per daemon running a requested output. For each, this
//! opens a zenoh *client* session to the daemon's endpoint, which forwards its
//! own nodes' samples to the clients linked to it, and subscribes there to the
//! source's outputs. Each sample is rebuilt the way the daemon rebuilds the
//! relayed ones ([`prepare_debug_topic_frame`]) and handed to the subscription
//! as the same `Timestamped<InterDaemonEvent::Output>` bytes a relayed frame
//! carries, so `topic echo`/`hz`/`info` can't tell the two apart.
//!
//! A message can also take the daemon path instead of the node's own zenoh
//! publisher: every message of an output the daemon pins there
//! (`OutputRouting::daemon_only`, e.g. one with a `queue_policy: backpressure`
//! consumer), and the ones sent before an output's direct route is proven. In
//! a debug dataflow the daemon publishes those on the output's control key, as
//! the relayed frame bytes, while anything subscribes there (`send_out`), so
//! that key is subscribed to as well. Each message takes one of the two keys.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use dora_core::{
    debug_topic::{DebugSchemaCache, prepare_debug_topic_frame, retain_published_schema},
    topics::{
        zenoh_daemon_control_topic, zenoh_endpoint_is_loopback, zenoh_output_publish_topic,
        zenoh_output_schema_topic,
    },
    uhlc::HLC,
};
use dora_message::{
    common::Timestamped,
    coordinator_to_cli::ZenohTopicSource,
    daemon_to_daemon::InterDaemonEvent,
    id::{DataId, NodeId},
    metadata::Metadata,
};
use eyre::{Context, eyre};
use uuid::Uuid;
use zenoh::{handlers::FifoChannelHandler, pubsub::Subscriber, sample::Sample};

use crate::ws_client::TopicSubscriber;

/// How long to wait for a daemon's zenoh endpoint before using the relay.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Env var that turns direct subscriptions off (`off`, `0`, `false`, `no`),
/// for a network where the endpoints are reachable but should not be used.
const DIRECT_ENV: &str = "DORA_TOPIC_DIRECT";

/// Whether to ask the coordinator for a direct subscription.
pub(crate) fn enabled() -> bool {
    !std::env::var(DIRECT_ENV).is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false" | "no"
        )
    })
}

/// Whether this CLI reached the coordinator on the coordinator's own host,
/// by the rule the coordinator applies to it (`peer_ip`): over loopback, or by
/// dialing one of this host's own addresses.
pub(crate) fn on_coordinator_host(coordinator: SocketAddr, local: SocketAddr) -> bool {
    let ip = coordinator.ip().to_canonical();
    ip.is_loopback() || ip == local.ip().to_canonical()
}

/// Subscribe to every source's outputs and start forwarding them to
/// `subscriber`.
///
/// Fails, handing `subscriber` back for the relay, if a source's endpoint
/// can't be dialed from here or reached, or a subscription can't be declared;
/// nothing is left running then. Once it succeeds, the forwarding tasks run
/// until the receiver is dropped.
pub(crate) async fn subscribe(
    dataflow_id: Uuid,
    sources: Vec<ZenohTopicSource>,
    on_coordinator_host: bool,
    subscriber: TopicSubscriber,
) -> Result<(), (eyre::Report, TopicSubscriber)> {
    // The coordinator hands a loopback endpoint only to a client it sees as
    // local, which a CLI behind a reverse proxy or tunnel on its host looks
    // like too. Such an endpoint would dial this CLI's own host instead.
    if !on_coordinator_host
        && let Some(source) = sources
            .iter()
            .find(|source| zenoh_endpoint_is_loopback(&source.endpoint))
    {
        let err = eyre!(
            "zenoh endpoint `{}` is on the coordinator's loopback, \
             which this CLI did not reach the coordinator over",
            source.endpoint
        );
        return Err((err, subscriber));
    }
    let outputs = match open_sources(dataflow_id, sources).await {
        Ok(outputs) => outputs,
        Err(err) => return Err((err, subscriber)),
    };
    let subscriber = Arc::new(subscriber);
    let clock = Arc::new(HLC::default());
    for output in outputs {
        tokio::spawn(output.forward(dataflow_id, subscriber.clone(), clock.clone()));
    }
    Ok(())
}

/// One subscribed output, with what keeps its subscriptions alive.
struct DirectOutput {
    node_id: NodeId,
    output_id: DataId,
    /// The samples the node publishes itself.
    data: Subscriber<FifoChannelHandler<Sample>>,
    /// The messages that take the daemon path, already as relayed frames.
    daemon_routed: Subscriber<FifoChannelHandler<Sample>>,
    schema_cache: DebugSchemaCache,
    /// Dropping it would undeclare the `@schema` subscription.
    _schema: zenoh_ext::AdvancedSubscriber<()>,
    /// Shared by the outputs of one source; the session closes with the last.
    _session: zenoh::Session,
}

async fn open_sources(
    dataflow_id: Uuid,
    sources: Vec<ZenohTopicSource>,
) -> eyre::Result<Vec<DirectOutput>> {
    let mut outputs = Vec::new();
    for source in sources {
        let session = open_client_session(&source.endpoint)
            .await
            .wrap_err_with(|| format!("failed to reach zenoh endpoint `{}`", source.endpoint))?;
        for (node_id, output_id) in source.topics {
            outputs.push(subscribe_output(&session, dataflow_id, node_id, output_id).await?);
        }
    }
    Ok(outputs)
}

/// A zenoh session that links to `endpoint` only: client mode, so it opens no
/// listener and joins no mesh, and no multicast scouting.
async fn open_client_session(endpoint: &str) -> eyre::Result<zenoh::Session> {
    let mut config = zenoh::Config::default();
    let insert = |config: &mut zenoh::Config, key: &str, value: &str| {
        config
            .insert_json5(key, value)
            .map_err(|e| eyre!("invalid zenoh config `{key}`: {e}"))
    };
    insert(&mut config, "mode", r#""client""#)?;
    // Serialized rather than interpolated: the endpoint came over the wire.
    insert(
        &mut config,
        "connect/endpoints",
        &serde_json::to_string(&[endpoint])?,
    )?;
    insert(
        &mut config,
        "connect/timeout_ms",
        &CONNECT_TIMEOUT.as_millis().to_string(),
    )?;
    insert(&mut config, "scouting/multicast/enabled", "false")?;
    // zenoh gives up after `connect/timeout_ms`; this only guards against it
    // not doing so.
    tokio::time::timeout(CONNECT_TIMEOUT * 2, zenoh::open(config))
        .await
        .map_err(|_| eyre!("timed out"))?
        .map_err(|e| eyre!(e))
}

async fn subscribe_output(
    session: &zenoh::Session,
    dataflow_id: Uuid,
    node_id: NodeId,
    output_id: DataId,
) -> eyre::Result<DirectOutput> {
    use zenoh_ext::{AdvancedSubscriberBuilderExt, HistoryConfig};

    let topic = zenoh_output_publish_topic(dataflow_id, &node_id, &output_id);
    let data = session
        .declare_subscriber(topic.clone())
        .await
        .map_err(|e| eyre!(e))
        .wrap_err_with(|| format!("failed to subscribe to `{topic}`"))?;
    let control_topic = zenoh_daemon_control_topic(dataflow_id, &node_id, &output_id);
    let daemon_routed = session
        .declare_subscriber(control_topic.clone())
        .await
        .map_err(|e| eyre!(e))
        .wrap_err_with(|| format!("failed to subscribe to `{control_topic}`"))?;

    // Small outputs are schema-once batches: the schema is published once on
    // the `@schema` key. Fetch it with history, as the daemon's relay does, so
    // the batches can be rebuilt even when the producer started earlier.
    let schema_cache: DebugSchemaCache = Default::default();
    let schema_topic = zenoh_output_schema_topic(dataflow_id, &node_id, &output_id);
    let schema = {
        let cache = schema_cache.clone();
        session
            .declare_subscriber(schema_topic.clone())
            .history(HistoryConfig::default().detect_late_publishers())
            .callback(move |sample| {
                retain_published_schema(
                    &mut cache.lock().unwrap_or_else(|e| e.into_inner()),
                    &sample.payload().to_bytes(),
                );
            })
            .await
            .map_err(|e| eyre!(e))
            .wrap_err_with(|| format!("failed to subscribe to `{schema_topic}`"))?
    };

    Ok(DirectOutput {
        node_id,
        output_id,
        data,
        daemon_routed,
        schema_cache,
        _schema: schema,
        _session: session.clone(),
    })
}

impl DirectOutput {
    async fn forward(self, dataflow_id: Uuid, subscriber: Arc<TopicSubscriber>, clock: Arc<HLC>) {
        loop {
            let frame = tokio::select! {
                sample = self.data.recv_async() => match sample {
                    Ok(sample) => self.frame(dataflow_id, &sample, &clock),
                    Err(_) => break,
                },
                sample = self.daemon_routed.recv_async() => match sample {
                    Ok(sample) => daemon_routed_frame(&sample),
                    Err(_) => break,
                },
            };
            let Some(frame) = frame else {
                continue;
            };
            if subscriber.offer(&frame).is_err() {
                break; // the receiver is gone
            }
        }
    }

    /// The relayed frame for `sample`, or `None` to skip it.
    fn frame(&self, dataflow_id: Uuid, sample: &Sample, clock: &HLC) -> Option<Vec<u8>> {
        // A node publishes the raw payload with its encoded metadata as the
        // attachment (see `DoraNode::zenoh_publish`).
        let metadata = sample
            .attachment()
            .and_then(|a| dora_message::decode::<Metadata>(&a.to_bytes()).ok())?;
        // Startup-handshake markers ride the data key with an empty payload.
        if metadata.is_startup_marker() {
            return None;
        }
        let payload = sample.payload().to_bytes();
        let (metadata, data) = prepare_debug_topic_frame(
            &mut self.schema_cache.lock().unwrap_or_else(|e| e.into_inner()),
            true,
            metadata,
            &payload,
        )?;
        let event = Timestamped {
            inner: InterDaemonEvent::Output {
                dataflow_id,
                node_id: self.node_id.clone(),
                output_id: self.output_id.clone(),
                metadata,
                data: Some(aligned_vec::AVec::from_slice(128, &data)),
            },
            timestamp: clock.new_timestamp(),
        };
        match event.serialize() {
            Ok(frame) => Some(frame),
            Err(err) => {
                tracing::debug!("failed to encode a direct topic frame: {err:?}");
                None
            }
        }
    }
}

/// The relayed frame for a sample on an output's control key, which is the
/// sample itself, or `None` for the key's other events, such as
/// `OutputClosed`.
fn daemon_routed_frame(sample: &Sample) -> Option<Vec<u8>> {
    let bytes = sample.payload().to_bytes();
    let event = Timestamped::deserialize_inter_daemon_event(&bytes).ok()?;
    match event.inner {
        InterDaemonEvent::Output { metadata, .. } if !metadata.is_startup_marker() => {
            Some(bytes.into_owned())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ws_client::{TopicBufferLimits, topic_channel};

    /// An endpoint that can't be reached fails within the connect timeout and
    /// hands the subscriber back, still connected to its receiver, so the
    /// caller can relay into the same receiver instead.
    // zenoh refuses tokio's current-thread scheduler.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn an_unreachable_source_hands_the_subscriber_back() {
        let (subscriber, receiver) = topic_channel(TopicBufferLimits::LIVE);
        // Nothing listens on port 1.
        let sources = vec![ZenohTopicSource::new(
            "tcp/127.0.0.1:1".to_string(),
            vec![("camera".to_string().into(), "image".to_string().into())],
        )];

        let started = std::time::Instant::now();
        let (err, subscriber) = subscribe(Uuid::nil(), sources, true, subscriber)
            .await
            .expect_err("an unreachable endpoint must fail");

        assert!(
            started.elapsed() < CONNECT_TIMEOUT * 2,
            "{:?}",
            started.elapsed()
        );
        assert!(
            format!("{err:#}").contains("tcp/127.0.0.1:1"),
            "the error names the endpoint: {err:#}"
        );
        assert!(subscriber.offer(b"frame").is_ok());
        assert_eq!(receiver.recv().unwrap().unwrap(), b"frame");
    }

    /// A CLI that reached the coordinator from another host, e.g. through a
    /// reverse proxy on the coordinator's host, doesn't dial a loopback
    /// endpoint, which would be its own host's, and relays instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_loopback_endpoint_is_not_dialed_off_the_coordinators_host() {
        let (subscriber, receiver) = topic_channel(TopicBufferLimits::LIVE);
        let sources = vec![
            ZenohTopicSource::new(
                "tcp/10.0.0.5:7447".to_string(),
                vec![("camera".to_string().into(), "image".to_string().into())],
            ),
            ZenohTopicSource::new(
                "tcp/127.0.0.1:7447".to_string(),
                vec![("lidar".to_string().into(), "points".to_string().into())],
            ),
        ];

        let (err, subscriber) = subscribe(Uuid::nil(), sources, false, subscriber)
            .await
            .expect_err("a loopback endpoint must not be dialed");

        assert!(
            format!("{err:#}").contains("tcp/127.0.0.1:7447"),
            "the error names the endpoint: {err:#}"
        );
        assert!(subscriber.offer(b"frame").is_ok());
        assert_eq!(receiver.recv().unwrap().unwrap(), b"frame");
    }

    /// The same rule as the coordinator's `peer_ip`.
    #[test]
    fn the_coordinators_host_is_recognized_like_the_coordinator_does() {
        let addr = |s: &str| s.parse::<SocketAddr>().unwrap();
        let local = addr("192.168.1.9:50000");
        assert!(on_coordinator_host(addr("127.0.0.1:6013"), local));
        assert!(on_coordinator_host(addr("[::1]:6013"), local));
        assert!(on_coordinator_host(addr("[::ffff:127.0.0.1]:6013"), local));
        // This host's own LAN address.
        assert!(on_coordinator_host(addr("192.168.1.9:6013"), local));
        // Another host, e.g. a reverse proxy in front of the coordinator.
        assert!(!on_coordinator_host(addr("192.168.1.5:6013"), local));
    }

    /// End to end over a real zenoh link: a peer session stands in for the
    /// daemon and publishes what a daemon and its node do, and the frames
    /// that come out are the ones the relay would send.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_frames_match_what_the_relay_sends() {
        use dora_message::metadata::{Parameter, SCHEMA_HASH, fnv1a};
        use zenoh_ext::{AdvancedPublisherBuilderExt, CacheConfig, MissDetectionConfig};

        let dataflow_id = Uuid::new_v4();
        let node_id: NodeId = "camera".to_string().into();
        let output_id: DataId = "image".to_string().into();
        let clock = HLC::default();

        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let endpoint = format!("tcp/127.0.0.1:{port}");
        let mut config = zenoh::Config::default();
        config
            .insert_json5(
                "listen/endpoints",
                &serde_json::to_string(&[&endpoint]).unwrap(),
            )
            .unwrap();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        let daemon = zenoh::open(config).await.unwrap();

        // Published before the CLI subscribes: it must come from the history.
        let schema = b"SCHEMA-BLOCK".to_vec();
        let schema_publisher = daemon
            .declare_publisher(zenoh_output_schema_topic(dataflow_id, &node_id, &output_id))
            .sample_miss_detection(MissDetectionConfig::default())
            .cache(CacheConfig::default())
            .publisher_detection()
            .await
            .unwrap();
        schema_publisher.put(schema.clone()).await.unwrap();

        let (subscriber, receiver) = topic_channel(TopicBufferLimits::LIVE);
        let sources = vec![ZenohTopicSource::new(
            endpoint,
            vec![(node_id.clone(), output_id.clone())],
        )];
        subscribe(dataflow_id, sources, true, subscriber)
            .await
            .map_err(|(err, _)| err)
            .unwrap();

        let attachment = |metadata: &Metadata| dora_message::encode(metadata).unwrap();
        let control_event = |inner: InterDaemonEvent| {
            Timestamped {
                inner,
                timestamp: clock.new_timestamp(),
            }
            .serialize()
            .unwrap()
        };
        let data_topic = zenoh_output_publish_topic(dataflow_id, &node_id, &output_id);
        let control_topic = zenoh_daemon_control_topic(dataflow_id, &node_id, &output_id);
        let batch = b"schema-less-batch".to_vec();
        let mut rebuilt = schema.clone();
        rebuilt.extend_from_slice(&batch);

        let (mut from_node, mut from_daemon) = (false, false);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        // Declarations take a moment to reach the daemon side, so publish
        // until both kinds have come through.
        while !(from_node && from_daemon) {
            assert!(
                std::time::Instant::now() < deadline,
                "node frame: {from_node}, daemon-routed frame: {from_daemon}"
            );
            // What a node publishes: a startup marker and a schema-once batch.
            daemon
                .put(&data_topic, Vec::<u8>::new())
                .attachment(attachment(&Metadata::startup_marker(clock.new_timestamp())))
                .await
                .unwrap();
            let mut metadata = Metadata::new(clock.new_timestamp());
            metadata.parameters.insert(
                SCHEMA_HASH.to_string(),
                Parameter::Integer(fnv1a(&schema) as i64),
            );
            daemon
                .put(&data_topic, batch.clone())
                .attachment(attachment(&metadata))
                .await
                .unwrap();
            // What the daemon publishes on the control key.
            let closed = control_event(InterDaemonEvent::OutputClosed {
                dataflow_id,
                node_id: node_id.clone(),
                output_id: output_id.clone(),
            });
            daemon.put(&control_topic, closed).await.unwrap();
            let routed = control_event(InterDaemonEvent::Output {
                dataflow_id,
                node_id: node_id.clone(),
                output_id: output_id.clone(),
                metadata: Metadata::new(clock.new_timestamp()),
                data: Some(aligned_vec::AVec::from_slice(128, b"routed")),
            });
            daemon.put(&control_topic, routed).await.unwrap();

            tokio::time::sleep(Duration::from_millis(100)).await;
            while let Ok(frame) = receiver.recv_timeout(Duration::ZERO) {
                let frame = frame.unwrap();
                let event = Timestamped::deserialize_inter_daemon_event(&frame).unwrap();
                let InterDaemonEvent::Output {
                    dataflow_id: id,
                    node_id: node,
                    output_id: output,
                    metadata,
                    data,
                } = event.inner
                else {
                    panic!("only outputs are forwarded");
                };
                assert_eq!((id, &node, &output), (dataflow_id, &node_id, &output_id));
                assert!(!metadata.is_startup_marker());
                match data.as_deref() {
                    Some(b"routed") => from_daemon = true,
                    Some(data) => {
                        assert_eq!(data, rebuilt, "the batch is rebuilt with its schema");
                        assert!(!metadata.parameters.contains_key(SCHEMA_HASH));
                        from_node = true;
                    }
                    None => panic!("a frame without data"),
                }
            }
        }
    }

    #[test]
    fn direct_subscriptions_can_be_turned_off() {
        for (value, expected) in [
            (None, true),
            (Some("on"), true),
            (Some("off"), false),
            (Some(" OFF "), false),
            (Some("0"), false),
            (Some("false"), false),
            (Some("no"), false),
        ] {
            // SAFETY: this is the only test that touches `DORA_TOPIC_DIRECT`,
            // and it doesn't run concurrently with itself.
            unsafe {
                match value {
                    Some(v) => std::env::set_var(DIRECT_ENV, v),
                    None => std::env::remove_var(DIRECT_ENV),
                }
            }
            assert_eq!(enabled(), expected, "{value:?}");
        }
        unsafe { std::env::remove_var(DIRECT_ENV) };
    }
}
