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
- 无符号整数超出 Lua 有符号 64 位范围时返回十进制字符串，避免溢出。时间保留小数秒，PostgreSQL TIMESTAMPTZ 返回 UTC RFC3339，TIMETZ 保留时区偏移。
- SQLite 按每一行的实际存储类型解码，并保留 REAL 的双精度；内存数据库连接关闭前不会因空闲回收丢失数据。
- NUMERIC / DECIMAL / MONEY 仍未提供原生解码；需要时在 SQL 中显式转成文本。

## 响应和构建

SQLx 使用自己的响应令牌表，不解引用 Lua 传入的整数地址，并检查响应所属服务。未消费响应在约 5–6 分钟后回收；服务阻塞太久后再读取会收到过期错误。这个机制沿用现有 `send_integer_message` ABI，无需为本轮修改重新编译 Moon。

SQLx 使用协议号 23，各共享连接的服务保持一致；该编号应保留给 SQLx。

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
