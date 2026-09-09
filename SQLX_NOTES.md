# SQLx 修复说明

本轮代码修改限定在 SQLx 实现、Lua 包装层及其依赖声明。HTTP、MongoDB、WebSocket、Tiberius、公共 Lua 绑定和 Moon 宿主接口保持原有实现。`service/lrust_sqldriver.lua` 是之前新增的 SQLx 服务适配。

## 连接、队列和关闭

- 每个命名连接由 FIFO 执行器独占一条物理连接，正常请求复用连接和 SQLx 语句缓存；不再逐请求 acquire/release，也不再执行连接池取还时的健康检查。`lrust_sqldriver` 用多个独立连接组成池，相同 affinity 始终进入同一条 FIFO 队列。
- 不再每次请求前主动探活：空闲期间断链后，首次请求可能直接收到连接错误，后续请求才重连；不会为了隐藏断链而自动重放这次 SQL。
- 自动释放不再由连接注册表中的强引用阻止；同名连接替换时，旧处理器只清理自己的注册项。
- `close()` 等已接收请求处理完再关闭连接；排空后的正常协议关闭最多等待 1 秒，超时则丢弃连接。多次关闭共享句柄仍等待同一次关闭流程，排空没有单独的总超时。
- 请求超时、连接故障或 `max_rows` 超限时，先丢弃旧连接再返回，不等待未读完的 SQL 结果或连接池后台归还任务。单独使用 `ext.sqlx` 时，下一条已排队/新请求才尝试重连；driver 在清理旧句柄后为下一条请求建立新句柄。
- 重连保留请求队列，但不恢复会话设置、临时表或 SQLite 内存数据库。客户端断开不等于数据库端 SQL 已立即停止；需要跨故障严格保序的业务应使用数据库锁、版本检查等约束，不能只依赖 lane。
- 普通事务语句失败时，显式等待 `ROLLBACK` 后返回原错误；不把回滚拖延到下一个请求。回滚也受当前请求超时限制，回滚失败/超时可能取代原错误。
- `COMMIT` / `ROLLBACK` 自身失败时丢弃连接，保留该错误的 SQLSTATE 等元数据；不会把事务状态不确定的连接继续复用。
- 入队失败返回 `BUSY` / `CLOSED`，恢复请求计数；事务构建器不会因为队列满丢失内容。
- SQL 失败只报告或记录一次，不自动重放。超时或断线不能证明写入未提交，需要业务自行决定如何处理。
- 只有**建立连接失败**才触发按连接/lane 独立的指数退避：默认 250、500、1000、2000、4000、5000ms，之后封顶；从失败结束计时，成功建连后重置。普通 SQL 错误、查询超时、行数超限不启动这项冷却。到期后也不会后台主动重连，只有下一条请求才尝试。
- 冷却期间请求立即返回错误，不睡眠占用队首，不提交 SQL，也不保留请求以后重试。错误增加 `connect_failed=true` 和 `retry_after_ms`；实际建连失败保留原 `kind`/SQLSTATE，Rust 冷却响应为 `SOCKET`，driver 自己的冷却保留最近一次建连错误的类别/元数据。业务可以据此决定是否重试；到期不保证恢复。
- Rust 为已有 actor 内部重建保存退避状态；显式 `sqlx.try_connect` 仍每次单独尝试，不引入跨名字/服务的全局失败缓存。driver 另外为各 lane 的首次惰性建连、故障后新建句柄保存退避状态；若错误来自已有 Rust actor 的建连阶段，不会关闭 actor 而清空其退避状态。启动预连接失败仍中止启动。
- 不等待调用的建连失败日志默认每连接/lane 最多每 5 秒一条，下次同类日志汇总期间抑制条数；没有后续失败时不另启定时日志。普通 SQL 错误照常记录，有等待者的错误逐条响应。退避和日志限速可分别关闭。

```lua
local sqlx = require "ext.sqlx"
local db, err = sqlx.try_connect(database_url, "main", {
    connect_timeout = 5000,
    request_timeout = 30000,
    max_rows = 100000,
    queue_capacity = 100,
    reconnect_initial_delay = 250, -- 0 禁用退避
    reconnect_max_delay = 5000,    -- 正整数，且 >= reconnect_initial_delay
    reconnect_log_interval = 5000, -- 0 禁用建连失败日志限速
})
assert(db, err and err.message)
```

`request_timeout` 限制每个已开始执行的请求（含故障后的必要重连），不含队列等待时间。`max_rows` 限制单次查询的行数，不是字节数。`lrust_sqldriver` 的 `max_queue` 另行控制业务请求队列，默认 0 仍表示不限；本次没有改变默认值。

三个 `reconnect_*` 配置同样支持 driver 启动配置的顶层或 `opts`，顶层优先；单位都是毫秒，最大值 4294967295。容量值仍由使用方按并发、请求/结果大小和内存预算选择；队列长度只是瞬时快照，不能代替服务端入队时的容量检查。本轮不增加字节容量限制、不修改任何容量默认值。

## 参数和结果

- 普通 Lua 字符串统一作为 UTF-8 文本，不再根据 `{` / `[` 开头猜测 JSON。非法 UTF-8 返回错误，二进制用 `sqlx.bytes(value)`。
- JSON 使用 Lua 值：`sqlx.json({enabled = true})`。已有 JSON 文本应先解码；`sqlx.json("abc")` 表示 JSON 字符串值，而非原始 JSON 文档。
- 普通 table、`sqlx.json` 和 `sqlx.array("json/jsonb", ...)` 里的 JSON 直接从 Lua 构造拥有所有权的 `serde_json::Value`，之后由 SQLx 编码一次。不再走 table→JSON 文本→解析 Value→SQLx 编码，减少一次编码、一次解析和中间缓冲分配；未改公共 JSON 模块，也未引入原始 JSON 文本绑定。
- 保留空 table→`[]`、数组识别/稀疏槽→JSON null、整数对象键转字符串、同名字符串化键按 Lua 遍历顺序保留最后值，以及最多 64 层 table 的规则。浮点 JSON 的文本表示不承诺与旧编码器逐字一致。对象键由 Serde 正确转义，包含引号、反斜杠、换行和空键均支持；非法 UTF-8、非有限数字、非字符串/整数对象键和不支持的值明确返回参数错误，不默默丢弃键值。
- SQL NULL 参数可用 `sqlx.null("int8")`、`sqlx.null("text")` 等指定类型。默认及裸 `nil` 使用 text 类型；PostgreSQL 其他类型的 NULL 应显式指定。
- SQL NULL 结果保留为 NULL lightuserdata，可与 `require("json").null` 比较；不再导致字段从结果表消失。
- PostgreSQL 数组使用 `sqlx.array("int8", {1, 2, 3})`；数组中的 SQL NULL 元素使用 `json.null`。显式参数标记可跨 Moon 服务序列化。
- `query()` 返回行数组；`execute_wait()` 返回 `rows_affected` 和适用时的 `last_insert_id`；`execute()` 不等待结果。
- 多语句使用 `batch()` / `execute_batch()`，不接受绑定参数，也不承诺事务原子性。需要原子性时使用 `transaction()`。
- 事务语句支持 `table.pack(sql, ...)`，保留尾部 nil 参数；外层语句列表不接受空洞。
- 解码失败返回错误，不再默默替换成 0、空串或 nil；重复列名要求 SQL 别名。
- 无符号整数超出 Lua 有符号 64 位范围时返回十进制字符串，避免溢出。时间保留小数秒，PostgreSQL TIMESTAMPTZ 的常规年份返回 UTC RFC3339；特殊值和扩展年份见下文。TIMETZ 保留时区偏移。
- SQLite 按每一行的实际存储类型解码，并保留 REAL 的双精度；内存数据库连接关闭前不会因空闲回收丢失数据。
- NUMERIC / DECIMAL / MONEY 仍未提供原生解码；需要时在 SQL 中显式转成文本。

## 响应和构建

SQLx 使用自己的响应登记表，不解引用 Lua 传入的整数地址，并检查响应所属服务。每个等待请求附带一个 Lua userdata 响应租约，包装层使用 Lua 5.4 的 `<close>` 管理：等待成功、中断或异常退出时立即释放；服务 Lua 状态销毁时由 `__gc` 兜底。迟到结果直接丢弃，不再保留 5–6 分钟，也不会因固定 TTL 误删仍在等待的结果。释放的是响应，不会取消、回滚或重放已接收的 SQL。

`sqlx.response_stats()` 返回进程级 `{waiting, ready}`，分别表示等待完成和已完成尚未解码的响应数量；不是字节数，也不改变任何队列/行数/容量限制。原有 `sqlx.stats()` 返回结构不变。机制仍沿用 `send_integer_message` ABI，无需重编译 Moon，但 Rust 动态库和 Lua 包装层必须同时更新。直接操作 `rust.sqlx` 的内部接口时，必须保存第二个返回值并使用 `<close>`，业务代码应继续使用包装层。

## 2026-09-09 异常与日期安全修复

- SQLx 原生参数校验改为非抛出式 Lua 读取与 Rust `Result`，避免 Linux Lua `longjmp` 跳过连接句柄、SQL 字符串和事务参数的析构。事务构建失败不提交已有语句。
- 原生调用入口包含 Rust panic 防护，返回 `ERROR`，不重放请求；写入结果仍可能不确定。此保护不等价于进程耗尽内存或 `panic=abort` 时的恢复保证。
- PostgreSQL 日期/时间使用有范围检查的整数解码，不经过 Chrono 的溢出运算。保留 `infinity/-infinity`、`24:00:00`、TIMETZ 偏移及小数秒，支持 PG 的完整有限日期范围。延续 ISO 天文年份表示：`0000` 是公元前 1 年，大于 9999 的正年份带 `+`；普通年份的返回格式保持不变。
- 连接、FIFO、重连策略、不重放、队列容量及 `max_rows` 配置均未改变。

在 Moon 根目录运行独立原生回归（Windows/Linux，编译 C Lua，模拟连接，不运行 Moon 或数据库）：

```text
cargo test --manifest-path ext/lrust/test/native/Cargo.toml --offline --target-dir target/sqlx-native-tests -- --test-threads=1
```

首次运行要求 Cargo 中已缓存相关依赖；去掉 `--offline` 会下载测试依赖。`example/test_sqlx_pg.lua` 增加了真实 PG 特殊日期与极值检查，留给业务环境执行。

SQLx 使用协议号 23，各共享连接的服务保持一致；该编号应保留给 SQLx。

## 2026-09-09 Lua 内存失败边界补齐

- 新增 SQLx 私有的 `sqlx_lua.rs`。可能分配内存或抛出 Lua 错误的操作在小型 `lua_pcall` 跳板内执行；跳板只借用数据，不持有需要析构的 Rust 对象。Lua 错误先正常返回 Rust，再通过内部 unwind 释放请求、参数、JSON、数组、结果、错误对象和引用计数，最后才重新抛给 Lua。保留原始 Lua 内存错误，不自动重放 SQL。
- 保护范围包括响应租约、连接/事务 userdata、命名连接查找、统计快照、输入表遍历及所有 SQLx 结果输出路径，不只修复 `connect()`。普通参数错误和 Rust panic 的错误表在分配失败时也走同一释放流程。
- userdata 元表完整构造后才写入注册表，userdata 先初始化为空值并安装 `__gc`，之后才转移 Rust 所有权。创建到一半失败不会缓存缺少析构函数的元表；同一 Lua 状态恢复分配后可以继续创建和使用对象。
- userdata 校验及析构使用各类型唯一的静态地址作为注册表键，不再通过可能分配字符串的 `luaL_testudata` 查找。输入遍历在普通提前返回时同时清理 key/value，异常展开时保留原 Lua 错误对象。
- **构建必须保留默认的 `panic=unwind`**。SQLx 在 `panic=abort` 配置下直接拒绝编译，避免将这套清理机制编译成终止进程。此机制解决 Lua 分配器失败，不保证 Rust 全局分配器 OOM、进程被杀或持续内存不足时仍能继续服务。
- 热路径不构造额外的整份结果副本；标量直接压栈，字段的键、值、写表合并为一次保护调用。新增保护调用的吞吐量影响仍需真实业务压测，不宣称零开销。连接复用、语句缓存、lane/FIFO、重连及容量默认值不变；本次无需修改业务 Lua 调用。

新增 `test/sqlx_allocation_cases.rs`：使用生产 Rust 源码和 C Lua，逐个拒绝分配点，失败后在同一 Lua 状态重试，并统计测试线程的 Rust 内存分配/释放净额。覆盖响应登记、连接/事务 userdata、嵌套 JSON、数组、SQLx 错误解码、参数遍历、结果字段、panic 错误报告、命名连接和统计快照；另外连续拒绝 100 次建连入口分配。测试不启动或轮询数据库运行时，不运行 Moon。

本次 Windows/Linux 原生回归各 15 项通过；10 类分配扫描覆盖 175 个失败点，各次清理后请求级 Rust 分配/释放净额为 0，响应登记归零。同一 Lua 状态的重试、命名连接引用计数和统计快照也通过。Lua 包装层、driver 的 FIFO/重连/不重放/退出模拟及 SQLx-only 严格 Clippy 通过。这些结论不代表已执行真实 PG 行数据解码或生产负载测试。

### 上线验收边界

离线回归用于验证原生资源释放和错误恢复，不能代替真实 PostgreSQL 联调。正式发布前由使用方在隔离测试库完成 `example/test_sqlx_pg.lua`，并按真实部署配置验证：

1. 正常查询、绑定参数、字段类型和事务往返全部通过，原有 driver 保序行为符合业务预期。
2. 查询中断链、查询超时、重连失败和数据库重启时，旧连接不再复用，当前 SQL 不自动重放；写入结果不确定时走业务幂等/核对流程。
3. 服务退出能排空；请求结束和服务销毁后，隔离测试进程的 `response_stats()` 回到 `{waiting=0, ready=0}`。该统计不是字节数，也不统计已经消费、尚待关闭的租约对象。
4. 使用代表性并发、结果大小和峰值持续压测，结合进程 RSS、Lua 内存、连接数、错误率与延迟判断稳定性；分配器可能缓存内存，不能仅要求 RSS 每次下降到初始值。
5. 在目标 Linux 发行版上重新构建并部署 `librust.so`（Windows 为 `rust.dll`），使用配套 Lua 包装层；先灰度观察，准备可回退的完整版本，不在运行中的 Lua 状态热替换原生库。

容量配置继续由使用方选择，本次未增加容量约束。真实数据库故障测试和持续负载测试尚未由本次自动执行；完成上述验收后再扩大生产流量。

driver 对跨服务响应打包增加了保护：NaN、嵌套超过 Moon 序列化深度等结果会尝试返回简单 `DRIVER` 错误。兜底发送/日志失败也不会卡死 lane，后续请求和排空退出继续执行。序列化失败不能说明原 SQL 未生效，不能重放请求；直接调用 `ext.sqlx` 的字段类型没有因此改变。

在 `ext/lrust` 目录构建仅启用 SQLx 的 DLL：

```text
cargo build --release -p lib-lualib --no-default-features --features sqlx
```

将 `target/release/rust.dll` 同步到 Moon 的 `clib/rust.dll`，同时将 `lualib/sqlx.lua` 同步到 Moon 的 `lualib/ext/sqlx.lua`。两者必须配套更新。现有 premake 的默认构建仍会编译其他默认模块；本次没有修改它的配置。

在 Moon 根目录运行不需要数据库或 Moon 进程的 Lua 包装层检查：

```text
premake5 --file=ext/lrust/test/sqlx_wrapper_check.lua check_sqlx
```

本轮已完成 SQLx-only 编译、严格 Clippy 和模拟 Lua 包装层检查。未启动 `moon.exe`，未执行真实数据库测试。

新增可控时钟模拟覆盖 driver 冷却快速失败、指数上限/恢复重置、独立 lane、错误元数据、日志限速、0 禁用选项、保留原生 actor 和冷却中关闭。Rust 退避/日志门控的纯逻辑可独立验证（在 `ext/lrust` 下，不启动 Moon 或数据库）：

```text
rustc --edition 2024 --test test/sqlx_retry_check.rs -o target/sqlx_retry_check.exe
target/sqlx_retry_check.exe
```

PG 手动测试补充 JSON 特殊键、普通/显式 JSON/JSON 数组、空表/稀疏数组、64 位整数与深度边界、循环表和非法值；SQLx-only 编译与模拟检查不代替这部分真实绑定往返测试。未进行吞吐量/延迟压测。
