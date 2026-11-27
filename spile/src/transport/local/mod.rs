//! 本地 IPC：同一台机器上的进程间通信。
//!
//! 上层只认识一个 `LocalTransport`；后端由编译目标决定——Unix 走 UDS，Windows 走
//! named pipe。两套实现都要满足下面这份契约，契约之外的行为属于后端私有，
//! 上层不得依赖。
//!
//! # 契约（两平台一致）
//!
//! 1. **同名指向同一监听者**：同一个 [`LocalAddr`](crate::transport::LocalAddr)
//!    在两端解析到同一个地址。
//! 2. **活着的监听者不可被抢占**：第二个进程用同一个地址 `bind`，拿到
//!    [`AlreadyExists`](std::io::ErrorKind::AlreadyExists)，而不是静默变成第二个
//!    服务端。两个服务端抢同一个名字时连接会被随机分给其中一个，查起来毫无线索。
//! 3. **地址非法一律 `InvalidInput`**：路径太长、端点类型不对都归这一类，且发生在
//!    动任何字节之前。
//! 4. **`connect_timeout` 到期必返回 `TimedOut`**：不存在「无限等」的分支。
//! 5. **整条连接关闭后对端读到 `UnexpectedEof`**：`recv` 不返回半条消息，也不永远挂住。
//! 6. **并发上限是平台常量**：Unix 走 backlog，Windows 是 254 个实例。超限的连接会等，
//!    而不是被拒。
//! 7. **只有同一用户能连上来**：Unix 靠 umask，Windows 靠进程令牌的默认 DACL。
//!
//! # 明确不在契约里（后端私有）
//!
//! - **只 drop 写半会不会让对端看到 EOF**。Unix 会（`OwnedWriteHalf` 在 drop 时
//!   `shutdown(SHUT_WR)`）；Windows 不会（named pipe 的读写是同一个句柄，没有半关闭，
//!   连接的生命周期由句柄引用计数决定）。上层要么两半一起关，要么靠协议层的 `GoAway`
//!   ——会话层本来就是这么做的，所以这个差异漏不到调用方。
//! - **崩溃残留能不能自动清理**。Unix 需要（socket 文件会留下，确认没人 listen 才删），
//!   Windows 不需要（句柄一关名字就消失）。所以「清理」只在 Unix 后端存在。
//! - **名字长度上限**。Unix `sun_path` 104/108 字节，Windows 256 字符。各后端自己报错，
//!   错误种类已被契约 3 归一。
//!
//! # 平台差异（平台事实，不是设计选择）
//!
//! | | Unix | Windows |
//! |---|---|---|
//! | `LocalAddr` 载荷 | 真实 socket 文件路径 | pipe 名的**种子**（不解析相对路径） |
//! | 传播方式 | 文件系统 | 机器全局 pipe 命名空间 |
//! | 调试访问器 | `path()` | `pipe_name()` |

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub use unix::{LocalSink, LocalSource, LocalTransport};

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::{LocalSink, LocalSource, LocalTransport};

#[cfg(not(any(unix, windows)))]
compile_error!(
    "`local` 传输只有 Unix 与 Windows 两个后端；当前编译目标两者都不是。\
     关掉 `local` feature，或先为这个平台实现一个后端。"
);
