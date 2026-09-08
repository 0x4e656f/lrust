# SQLx 修复说明

本轮代码修改限定在 SQLx 实现、Lua 包装层及其依赖声明。HTTP、MongoDB、WebSocket、Tiberius、公共 Lua 绑定和 Moon 宿主接口保持原有实现。`service/lrust_sqldriver.lua` 是之前新增的 SQLx 服务适配。

## 连接、队列和关闭

- 每个命名连接使用单连接池和 FIFO 执行队列；`lrust_sqldriver` 用多个独立连接组成池，相同 affinity 始终进入同一条 FIFO 队列。
- 自动释放不再由连接注册表中的强引用阻止；同名连接替换时，旧处理器只清理自己的注册项。
- `close()` 等已接收请求处理完、连接池关闭后返回。多次关闭共享连接也等待同一次实际关闭。
- 入队失败返回 `BUSY` / `CLOSED`，恢复请求计数；事务构建器不会因为队列满丢失内容。
- SQL 失败只报告或记录一次，不自动重放。超时或断线不能证明写入未提交，需要业务自行决定如何处理。

```lua
local sqlx = require "ext.sqlx"
local db, err = sqlx.try_connect(database_url, "main", {
    connect_timeout = 5000,
    request_timeout = 30000,
    max_rows = 100000,
    queue_capacity = 100,
})
assert(db, err and err.message)
```

`request_timeout` 限制每个已开始执行的请求，不含队列等待时间。`max_rows` 限制单次查询的行数，不是字节数。`lrust_sqldriver` 的 `max_queue` 另行控制业务请求队列。

## 参数和结果

- 普通 Lua 字符串统一作为 UTF-8 文本，不再根据 `{` / `[` 开头猜测 JSON。非法 UTF-8 返回错误，二进制用 `sqlx.bytes(value)`。
- JSON 使用 Lua 值：`sqlx.json({enabled = true})`。已有 JSON 文本应先解码；`sqlx.json("abc")` 表示 JSON 字符串值，而非原始 JSON 文档。
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
