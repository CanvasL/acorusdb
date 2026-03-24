# AcorusDB

一个基于 Rust 和 Tokio 实现的轻量级 TCP Key-Value 数据库项目。

当前项目已经具备一条完整的最小闭环：

- 文本行协议
- 内存 KV 存储
- WAL 持久化
- snapshot 恢复
- 自动 compact
- tracing 日志
- 配置文件加载
- 优雅停机
- 单元测试与端到端测试

它现在还不是 Redis 兼容实现，也不是完整的 LSM Tree 存储引擎，而是一个结构清晰、适合继续演进的数据库原型。

## 当前能力

- 支持命令：
  - `PING`
  - `SET key value`
  - `GET key`
  - `EXISTS key`
  - `DEL key`
  - `EXIT`
  - `QUIT`
- 支持 `value` 中包含空格。
- `key` 当前不允许包含空白字符。
- 启动时会先加载 snapshot，再回放 WAL。
- WAL 达到阈值后会触发 compact。
- 收到 `Ctrl+C` 或 `SIGTERM` 后会停止接收新连接，并通知现有连接退出。

## 项目结构

- `src/lib.rs`
  - 库入口，导出核心模块。
- `src/main.rs`
  - 二进制入口，只负责 CLI、配置加载、tracing 初始化和启动 server。
- `src/server.rs`
  - 监听 TCP、接收连接、协调 shutdown。
- `src/session.rs`
  - 单连接读写循环。
- `src/protocol.rs`
  - 文本协议解析和响应输出。
- `src/database.rs`
  - 命令执行入口。
- `src/storage_engine.rs`
  - 内存状态、snapshot、WAL 和 compact 的协调层。
- `src/wal.rs`
  - WAL 读写、恢复和 reset。
- `src/snapshot.rs`
  - snapshot 保存和加载。
- `src/config.rs`
  - TOML 配置读取。
- `src/error.rs`
  - 统一内部错误类型。
- `src/shutdown.rs`
  - 停机信号处理。

## 运行方式

### 1. 直接启动

```bash
cargo run
```

默认会读取当前目录下的 `acorusdb.toml`。

### 2. 指定配置文件

```bash
cargo run -- --config ./acorusdb.toml
```

或者：

```bash
cargo run -- -c /path/to/acorusdb.toml
```

### 3. 查看帮助

```bash
cargo run -- --help
```

## 配置文件

默认配置文件示例：

```toml
[server]
bind_addr = "127.0.0.1:7634"

[logging]
level = "info"

[snapshot]
path = "acorusdb.snapshot"

[wal]
path = "acorusdb.wal"
compact_threshold_bytes = 1024
```

字段说明：

- `server.bind_addr`
  - TCP 监听地址。
- `logging.level`
  - tracing 日志级别，例如 `trace`、`debug`、`info`、`warn`、`error`。
- `snapshot.path`
  - snapshot 文件路径。
- `wal.path`
  - WAL 文件路径。
- `wal.compact_threshold_bytes`
  - WAL 大于这个字节阈值后会触发 compact。

## 协议说明

当前协议是简单的文本行协议，一行一个请求。

### 请求

```text
PING
SET name acorus db
GET name
EXISTS name
DEL name
EXIT
```

### 响应

```text
PONG
OK
acorus db
1
0
(nil)
BYE
ERR unknown command
ERR usage: SET key value
```

约定说明：

- `GET` 查不到返回 `(nil)`
- `EXISTS` / `DEL` 返回 `1` 或 `0`
- `EXIT` / `QUIT` 返回 `BYE` 后断开连接

## 调试示例

可以用 `nc` 直接连：

```bash
nc 127.0.0.1 7634
```

然后输入：

```text
PING
SET language rust
GET language
EXISTS language
DEL language
GET language
EXIT
```

## 持久化与恢复

当前写路径大致是：

```text
client command
  -> protocol parse
  -> database execute
  -> WAL append + sync
  -> apply to in-memory map
  -> maybe compact
```

当前恢复路径大致是：

```text
load snapshot
  -> replay WAL
  -> rebuild in-memory state
```

当前实现特点：

- WAL 每次写入后会 `flush + sync_all`
- snapshot 保存会走临时文件、rename 和目录同步
- WAL reset 也做了同步处理
- WAL 最后一行损坏会被视作可能的 torn write 并忽略
- WAL 中间行损坏会作为错误上报

## Tombstone 设计

当前项目已经把 delete 语义显式建模成 tombstone，并为后续 mini-LSM 演进做准备。

当前规则如下：

1. tombstone 表示“这个 key 被逻辑删除”，而不是“系统里从未出现过这个 key”。
2. 在 [`src/storage_engine.rs`](/Users/fan/MyProjects/acorusdb/src/storage_engine.rs) 中，内存表 `mem_table` 使用 `MemValue::Tombstone` 表示删除状态。
3. `GET` 遇到 tombstone 时返回不存在，也就是协议层的 `(nil)`。
4. `EXISTS` 遇到 tombstone 时返回 `false`，也就是协议层的 `0`。
5. 对已经是 tombstone 的 key 再执行一次 `DEL`，返回 `false`。
6. `SET` 可以覆盖 tombstone，使同一个 key 重新生效。
7. WAL 中的 `Delete` 在恢复时会重建成 tombstone，而不是直接把 key 从内存表里移除。
8. 当前 snapshot 也会持久化 tombstone，保证 compact 和重启后删除语义不丢失。
9. 当前 compact 不会主动清理 tombstone，它只是把当前 `mem_table` 状态落盘并清空 WAL。
10. 未来进入 SSTable / LSM 阶段后，tombstone 会继续承担“遮蔽旧层旧值”的职责，并在合适的 compaction 时机清理。

## 错误处理

项目内部错误统一放在 `AcorusError` 中，当前已经区分了这些主要场景：

- 配置文件读取和解析错误
- 监听地址 bind 失败
- shutdown 信号安装失败
- WAL 打开、读取、写入、reset 失败
- WAL 损坏
- snapshot 编码、写入、读取、解码失败

协议错误单独保留在 `protocol` 层，不和内部运行时错误混在一起。

## 优雅停机

收到 `Ctrl+C` 或 `SIGTERM` 后：

1. server 停止接收新连接
2. 向活跃 session 广播 shutdown
3. 连接收到 `BYE`
4. 等待 session 收尾
5. 进程退出

## 测试

运行测试：

```bash
cargo test
```

当前测试覆盖包括：

- 协议解析
- WAL 编解码
- `set/delete/restart` 恢复
- tombstone 重启恢复与重复删除语义
- `SET -> DEL -> SET` 之后的 key 复活语义
- compact 后恢复
- compact 后 tombstone 保留
- `snapshot + wal` 叠加恢复
- 重启后和 compact 后的 key 顺序稳定性
- WAL 损坏边界
- TCP 会话端到端读写
- shutdown 时客户端收到 `BYE`

## 当前限制

- 还不是 RESP 协议
- 还不是 Redis 客户端兼容实现
- 当前 snapshot 还不是 SSTable
- 当前存储结构还不是 LSM Tree
- 还没有 manifest、Bloom filter、后台 compaction 等能力

## 下一步方向

下一步计划已经整理在 [TODO.md](/Users/fan/MyProjects/acorusdb/TODO.md)。

核心方向是把当前存储引擎逐步演进成一个 mini-LSM：

- 先把 memtable 改成 `BTreeMap`
- 再把 snapshot 演进成 SSTable
- 然后支持 flush、多表读和 merge compaction
