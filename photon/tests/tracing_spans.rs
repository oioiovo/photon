//! M3 第十步验收：**一次调用在日志里是一个 span，而不是散落的几行**。
//!
//! 验的是四件事，缺一条这套东西就等于没做：
//! 1. 客户端有 `photon.call`，带 `method_name`（哈希认不出来的名字，必须由宏补上）
//! 2. 服务端有 `photon.request`，带同名 `method_name`
//! 3. 两个 span 的 `request_id` 相同——客户端那个是 `Session::call` 回填的
//! 4. handler 里打的日志**落在请求 span 里**（`instrument` 真的生效，而不只是建了个空 span）
//!
//! `tracing-subscriber` 只在 dev-dependency 里（design 3.1「观测」一行的约定）：
//! 主 crate 不引它，用户自己选 subscriber。

#![cfg(feature = "memory")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use photon::transport::memory::Memory;
use photon::{ServiceRegistry, Session, SessionOptions};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry;

#[photon::service(id = 41)]
trait Probe {
    async fn add(&self, a: i64, b: i64) -> Result<i64, photon::RpcError>;
}

#[photon::service(id = 42, blocking)]
trait ProbeSync {
    fn add(&self, a: i64, b: i64) -> Result<i64, photon::RpcError>;
}

struct ProbeSyncImpl;

impl ProbeSync for ProbeSyncImpl {
    fn add(&self, a: i64, b: i64) -> Result<i64, photon::RpcError> {
        tracing::info!("inside-blocking-handler");
        Ok(a + b)
    }
}

struct ProbeImpl;

impl Probe for ProbeImpl {
    async fn add(&self, a: i64, b: i64) -> Result<i64, photon::RpcError> {
        // 这条日志必须出现在 `photon.request` 的 span 里，否则 instrument 没生效。
        tracing::info!("inside-handler");
        Ok(a + b)
    }
}

// ---------------------------------------------------------------- 抓 span 的 Layer

#[derive(Default)]
struct Record0 {
    name: String,
    fields: BTreeMap<String, String>,
    events: Vec<String>,
}

#[derive(Default)]
struct Collected {
    spans: Vec<Record0>,
    /// `span id` → `spans` 下标。`on_record` 只有 id，得能找回那条记录。
    by_id: std::collections::HashMap<u64, usize>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Collected>>);

impl Capture {
    fn snapshot(&self) -> Vec<(String, BTreeMap<String, String>, Vec<String>)> {
        let c = self.0.lock().unwrap();
        c.spans
            .iter()
            .map(|s| (s.name.clone(), s.fields.clone(), s.events.clone()))
            .collect()
    }
}

/// 把 span / event 上的字段收进 `BTreeMap`。
struct Grabber<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Grabber<'_> {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl<S> Layer<S> for Capture
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Grabber(&mut fields));
        let mut c = self.0.lock().unwrap();
        let idx = c.spans.len();
        c.spans.push(Record0 {
            name: attrs.metadata().name().to_string(),
            fields,
            events: Vec::new(),
        });
        c.by_id.insert(id.into_u64(), idx);
    }

    /// `request_id` 是在 `Session::call` 里**后填**的，所以这里必须跟着更新，
    /// 否则断言「两个 span 的 request_id 相同」永远看到空字段。
    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let mut c = self.0.lock().unwrap();
        let Some(&idx) = c.by_id.get(&id.into_u64()) else {
            return;
        };
        let mut extra = BTreeMap::new();
        values.record(&mut Grabber(&mut extra));
        c.spans[idx].fields.extend(extra);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // `event_span` 给出「这条 event 归属哪个 span」，正是 instrument 的验证点。
        let Some(span) = ctx.event_span(event) else {
            return;
        };
        let mut fields = BTreeMap::new();
        event.record(&mut Grabber(&mut fields));
        let message = fields.remove("message").unwrap_or_default();
        let mut c = self.0.lock().unwrap();
        if let Some(&idx) = c.by_id.get(&span.id().into_u64()) {
            c.spans[idx].events.push(message);
        }
    }
}

/// 一个进程只能装一次全局 subscriber，所以整份文件共用它。
fn capture() -> &'static Capture {
    static ONCE: OnceLock<Capture> = OnceLock::new();
    ONCE.get_or_init(|| {
        let cap = Capture::default();
        let subscriber = registry().with(cap.clone());
        // 多线程 runtime 下 `with_default` 的线程局部 dispatcher 不会跟着任务跑，
        // 只能用全局——这也正是真实部署的形态。
        tracing::subscriber::set_global_default(subscriber).expect("subscriber 只装一次");
        cap
    })
}

// ---------------------------------------------------------------- 用例

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_call_is_one_span_with_a_name() {
    let cap = capture();

    // 服务端：注册 `Probe`，跑在 memory 的另一端。
    let ((client_src, client_sink), (server_src, server_sink)) = Memory::pair();
    let mut registry = ServiceRegistry::empty();
    register_probe(&mut registry, Arc::new(ProbeImpl)).expect("注册不应撞号");

    let server = Session::new(
        server_src,
        server_sink,
        Arc::new(registry),
        SessionOptions::default(),
    )
    .expect("服务端会话")
    .0;
    let server_task = tokio::spawn(async move {
        let _ = server.wait_closed().await;
    });

    let opts = SessionOptions::default();
    let (_, handle) = Session::new(
        client_src,
        client_sink,
        Arc::new(ServiceRegistry::empty()),
        opts,
    )
    .expect("客户端会话");
    let client = ProbeClient::new(handle);

    let sum = client.add(20, 22).await.expect("调用应成功");
    assert_eq!(sum, 42);

    let snapshot = cap.snapshot();

    let call = snapshot
        .iter()
        .find(|(name, ..)| name == "photon.call")
        .expect("客户端应有 photon.call span");
    assert_eq!(call.1.get("service_id").map(String::as_str), Some("41"));
    assert_eq!(call.1.get("method_name").map(String::as_str), Some("add"));
    let client_request_id = call
        .1
        .get("request_id")
        .expect("request_id 应由 Session::call 回填");

    let request = snapshot
        .iter()
        .find(|(name, ..)| name == "photon.request")
        .expect("服务端应有 photon.request span");
    assert_eq!(request.1.get("service_id").map(String::as_str), Some("41"));
    assert_eq!(
        request.1.get("method_name").map(String::as_str),
        Some("add")
    );
    assert_eq!(request.1.get("code").map(String::as_str), Some("0"));
    assert_eq!(
        request.1.get("request_id"),
        Some(client_request_id),
        "两个 span 必须是同一个 request_id"
    );

    // instrument 真的生效：handler 里打的日志落在请求 span 里，而不是散在根上。
    assert!(
        request.2.iter().any(|m| m.contains("inside-handler")),
        "handler 日志没有落在 photon.request span 里：{:?}",
        request.2
    );

    server_task.abort();
}

/// 同步路径的 span 在会话线程上建（调用方线程上没有 runtime，也没有 `await`），
/// 所以名字必须由 stub 递过去；这条用例锁住「两条路径同名同字段」。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_call_also_carries_the_name() {
    let cap = capture();

    let ((client_src, client_sink), (server_src, server_sink)) = Memory::pair();
    let mut registry = ServiceRegistry::empty();
    register_probe_sync(&mut registry, Arc::new(ProbeSyncImpl)).expect("注册不应撞号");

    let server = Session::new(
        server_src,
        server_sink,
        Arc::new(registry),
        SessionOptions::default(),
    )
    .expect("服务端会话")
    .0;
    let server_task = tokio::spawn(async move {
        let _ = server.wait_closed().await;
    });

    // `from_parts` 自己起线程与 runtime：调用方不需要 runtime，也不能在这里 `block_on`。
    let session = photon::blocking::BlockingSession::from_parts(
        client_src,
        client_sink,
        SessionOptions::default(),
    )
    .expect("同步客户端会话");
    let client = ProbeSyncBlockingClient::new(session);
    assert_eq!(client.add(20, 22).expect("调用应成功"), 42);

    let snapshot = cap.snapshot();
    let mine: Vec<_> = snapshot
        .iter()
        .filter(|(_, fields, _)| fields.get("service_id").map(String::as_str) == Some("42"))
        .collect();
    let call = mine
        .iter()
        .find(|(name, ..)| name == "photon.call")
        .expect("同步客户端也应有 photon.call span");
    assert_eq!(call.1.get("method_name").map(String::as_str), Some("add"));
    let request = mine
        .iter()
        .find(|(name, ..)| name == "photon.request")
        .expect("服务端应有 photon.request span");
    assert_eq!(
        request.1.get("method_name").map(String::as_str),
        Some("add")
    );
    assert_eq!(request.1.get("request_id"), call.1.get("request_id"));
    assert!(
        request
            .2
            .iter()
            .any(|m| m.contains("inside-blocking-handler")),
        "同步 handler 的日志没有落在请求 span 里：{:?}",
        request.2
    );

    server_task.abort();
}
