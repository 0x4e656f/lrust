--- SQLx 的 Moon 协程包装层，通过 require("ext.sqlx") 使用；本模块不是独立服务。
--- sqlx.connect/try_connect 创建连接，返回的 db 使用冒号调用 query/execute/transaction/close。
--- 每个命名连接只有一条 FIFO 执行队列，按请求实际入队顺序串行处理；不会自动重放 SQL。
--- 正常请求复用专用物理连接和语句缓存，不逐请求取还连接；超时/断线/行数超限后丢弃旧连接。
--- 故障后的下一条请求才重建连接；会话设置、临时表等不会恢复，SQLite 内存库也可能丢失。
--- 客户端超时/断线不保证数据库端 SQL 已停止，不能据此保证跨故障的业务执行顺序。
--- 需要连接池或按业务 key 保序时使用 lrust_sqldriver.client，不要跨服务传递 db 对象。
--- 标有 @async 的接口需要在允许挂起的 Moon 协程中调用；等待数据库不会阻塞整个工作线程。
--- query/execute_wait/batch/transaction 失败返回带 kind/message 的错误表，不是 nil, err。
--- try_connect/close 使用双返回值报告错误；connect 则在连接失败时抛出 Lua 错误。
--- 本模块占用 Moon 协议号 23；参数、返回值和具体限制见各函数注释。
---@diagnostic disable: inject-field, undefined-global
local moon = require "moon"
---@type any
local c = require "rust.sqlx"
assert(type(c.response_stats) == "function", "SQLx native/wrapper version mismatch: update rust.dll and sqlx.lua together")

-- Keep one stable protocol across services sharing a named connection.
local protocol_type = 23

moon.register_protocol {
    name = "lrust_sqlx",
    PTYPE = protocol_type,
    pack = function(...) return ... end,
    unpack = function(val)
        return c.decode(val, moon.id)
    end
}

--- SQLx 参数、结果和配置类型；供 SQLx 包装层与 lrust_sqldriver 共同引用。
--- SQL NULL 使用 json.null（空 lightuserdata），不是 Lua nil；日期/时间/UUID 返回字符串。
--- PG 日期支持 infinity/-infinity 与完整有限范围；TIME/TIMETZ 保留 24:00:00。
--- 日期采用 ISO 天文年份（0000 为公元前 1 年），超出四位数的正年份带 +。
---@alias SqlXNullValue userdata 仅指 json.null（空 lightuserdata），不是任意 userdata。
---@alias SqlXValue boolean|number|string|SqlXNullValue|table
---@alias SqlXRow table<string, SqlXValue>
---@alias SqlXUInt64 integer|string 非负整数；超过 Lua 有符号 64 位上限时使用十进制字符串。
---@alias SqlXJsonValue boolean|number|string|SqlXNullValue|table|nil JSON 值；userdata 仅允许 json.null。
---@alias SqlXArrayValue boolean|number|string|SqlXNullValue|table 一维数组元素；SQL NULL 元素使用 json.null。

---@alias SqlXNullType
---| 'bool' | 'boolean'
---| 'int2' | 'smallint' | 'int4' | 'int' | 'integer' | 'int8' | 'bigint'
---| 'float4' | 'real' | 'float8' | 'double' | 'double precision'
---| 'text' | 'string' | 'varchar' | 'char'
---| 'bytes' | 'bytea' | 'blob' | 'binary'
---| 'json' | 'jsonb' | 'uuid' | 'date'
---| 'timestamp' | 'datetime' | 'timestamptz' | 'timestamp with time zone'
---| 'time' | 'timetz' | 'time with time zone'

---@alias SqlXArrayType
---| 'bool' | 'boolean'
---| 'int2' | 'smallint' | 'int4' | 'int' | 'integer' | 'int8' | 'bigint'
---| 'float4' | 'real' | 'float8' | 'double' | 'double precision'
---| 'text' | 'varchar' | 'char' | 'character varying' | 'name'
---| 'bytea' | 'bytes' | 'uuid' | 'json' | 'jsonb'

---@class SqlXNullParam
---@field __sqlx_param 'null'
---@field type SqlXNullType|string SQL 类型名，默认 text；运行时忽略大小写和首尾空格。

---@class SqlXTextParam
---@field __sqlx_param 'text'
---@field value string UTF-8 文本；不根据内容自动识别 JSON。

---@class SqlXJsonParam
---@field __sqlx_param 'json'
---@field value? SqlXJsonValue Lua JSON 值；nil/json.null 表示 JSON null，SQL NULL 使用 sqlx.null("jsonb")。

---@class SqlXBytesParam
---@field __sqlx_param 'bytes'
---@field value string 原始字节串，允许 NUL 和非 UTF-8 字节。

---@class SqlXArrayParam
---@field __sqlx_array true
---@field type SqlXArrayType|string 元素类型；忽略大小写、首尾空格，并允许 [] 后缀。
---@field values SqlXArrayValue[] 无空洞的一维序列；空表表示空数组，json.null 表示 NULL 元素。

--- 普通 table 作为 JSON；参数包装器显式指定 NULL、文本、JSON、字节或 PG 数组。
--- 普通 string 绑定 TEXT；裸 nil/json.null 绑定 TEXT 类型的 SQL NULL。
---@alias SqlXParam boolean|number|string|SqlXNullValue|table|SqlXNullParam|SqlXTextParam|SqlXJsonParam|SqlXBytesParam|SqlXArrayParam|nil

---@class SqlXStatement
---@field [integer] string|SqlXParam # [1] 为 SQL 字符串，[2..n] 为绑定参数。
---@field n? integer table.pack 的总长度，包含 SQL；有尾部 nil 时必须保留。

---@class SqlXConnectOptions
---@field connect_timeout? integer 建立连接超时，毫秒，默认 5000，必须大于 0。
---@field request_timeout? integer 单个请求开始执行后的超时，毫秒，默认 30000；含必要重连，不含排队时间。
---@field max_rows? integer 单次 query 最大返回行数，默认 100000；不是字节数限制。
---@field queue_capacity? integer Rust 连接待执行队列容量，默认 100；不含正在执行的请求。
---@field reconnect_initial_delay? integer 已有连接重建失败后的首次冷却毫秒数，默认 250；0 禁用退避。
---@field reconnect_max_delay? integer 连续建连失败的最大冷却毫秒数，默认 5000；正整数且不小于 reconnect_initial_delay。
---@field reconnect_log_interval? integer 不等待调用的建连失败日志间隔，毫秒，默认 5000；0 不限速，不影响普通 SQL 错误。

---@alias SqlXErrorKind 'ERROR'|'DB'|'SOCKET'|'TIMEOUT'|'BUSY'|'CLOSED'

---@class SqlXError
---@field connect_failed? boolean true 表示本次在建连/冷却阶段失败，未提交 SQL；显式 connect 失败也设置此字段。
---@field retry_after_ms? integer 建连冷却的剩余毫秒数；仅为本连接的建议，不代表数据库已恢复，不会自动重试。
---@field kind SqlXErrorKind 错误类别；TIMEOUT/SOCKET 不代表写入一定没有提交。
---@field message string 错误说明。
---@field error_kind? string 数据库约束等错误分类（例如 UniqueViolation）。
---@field sqlstate? string 数据库提供时包含 SQLSTATE。
---@field constraint? string 数据库提供时包含约束名。
---@field table? string 数据库提供时包含表名。

---@class SqlXExecuteResult
---@field message 'ok'
---@field rows_affected SqlXUInt64 影响行数；batch 为多条语句的合计。
---@field last_insert_id? SqlXUInt64 MySQL/SQLite 适用；PostgreSQL 无此字段，使用 RETURNING。

---@class SqlXTransactionResult
---@field message 'ok'
---@field rows_affected SqlXUInt64 事务内语句的累计影响行数，不包含逐条查询结果。

---@alias SqlXQueryResult SqlXRow[]|SqlXError
---@alias SqlXWriteResult SqlXExecuteResult|SqlXError
---@alias SqlXTransactionResponse SqlXTransactionResult|SqlXError
---@alias SqlXStats table<string, integer> 命名连接到请求数的映射，包含排队和执行中请求。

--- Rust userdata 的内部接口；业务代码通过 SqlX 包装层调用。
--- 原生异步接口的第二个返回值为响应租约，必须保存到等待结束并使用 <close>。
--- 包装层已自动管理；不要单独更新 Lua 文件而仍加载旧版 rust.dll。
---@alias SqlXResponseLease userdata

---@class SqlXNativeTransaction
---@field push fun(self: SqlXNativeTransaction, sql: string, ...: SqlXParam): SqlXError?

---@class SqlXNativeConnection
---@field query fun(self: SqlXNativeConnection, owner: integer, session: integer, sql: string, ...: SqlXParam): integer|SqlXError, SqlXResponseLease?
---@field execute fun(self: SqlXNativeConnection, owner: integer, session: integer, sql: string, ...: SqlXParam): integer|SqlXError, SqlXResponseLease?
---@field batch fun(self: SqlXNativeConnection, owner: integer, session: integer, sql: string): integer|SqlXError, SqlXResponseLease?
---@field transaction fun(self: SqlXNativeConnection, owner: integer, session: integer, transaction: SqlXNativeTransaction): integer|SqlXError, SqlXResponseLease?
---@field close fun(self: SqlXNativeConnection, protocol: integer, owner: integer, session: integer): integer|SqlXError, SqlXResponseLease?

--- 模块同时作为连接对象的方法表。每个命名连接串行执行已接收的请求。
---@class SqlX
---@field obj? SqlXNativeConnection 内部连接句柄，请勿直接操作；成功 close 后清空。
local M = {}

--- 构造统一错误表；供本地校验、等待中断和已关闭句柄等失败路径使用。
---@param kind SqlXErrorKind 错误分类。
---@param message any 错误内容，统一转换为字符串。
---@return SqlXError result 含 kind 和 message 的错误表。
local function failure(kind, message)
    return { kind = kind, message = tostring(message) }
end

--- 等待原生异步请求的响应；若入队阶段已返回错误表，则直接透传且不挂起。
--- Moon 等待被中断时转成 ERROR；这里只等待会话，不额外设置超时或重试请求。
---@async
---@param session integer|SqlXError 会话 ID，或未入队就产生的错误。
---@param guard? SqlXResponseLease 原生请求附带的响应租约，等待退出时自动释放。
---@return any result 会话协议解码后的动态响应；具体返回类型由公开接口约束。
local function wait_result(session, guard)
    -- __close also runs on an interrupted wait or Lua exception; __gc handles
    -- service destruction. This abandons the reply, never the accepted SQL.
    local response_guard <close> = guard
    if type(session) == "table" then
        return session
    end
    local res, err = moon.wait(session)
    if res == nil or res == false then
        return failure("ERROR", err or "SQLx wait interrupted")
    end
    return res
end

--- 校验包装对象并调用原生方法提交请求；本函数本身不等待数据库执行。
--- 已清空的句柄返回 CLOSED；原生参数校验等 Lua 异常转成 ERROR，入队错误直接透传。
---@param self SqlX
---@param method 'query'|'execute'|'batch'|'transaction'
---@param session integer 0 表示不回传结果。
---@param ... any 原生方法参数：SQL/绑定值或事务 userdata。
---@return integer|SqlXError
local function invoke(self, method, session, ...)
    local obj = self.obj
    if obj == nil then
        return failure("CLOSED", "database connection is closed")
    end
    local ok, res, guard = pcall(obj[method], obj, moon.id, session, ...)
    if not ok then
        return failure("ERROR", res)
    end
    return res, guard
end

--- 记录不等待接口在提交阶段遇到的错误；正常提交不输出日志。
--- 已入队请求后续的数据库错误/超时由 Rust 执行队列记录，不经过这里。
---@param res integer|SqlXError 会话标记或提交错误。
local function log_error(res)
    if type(res) == "table" and res.kind then
        moon.error(string.format("SQLx [%s]: %s", res.kind, res.message))
    end
end

local pg_array_metatable = {
    __sqlx_array = true,
}

local sqlx_param_metatable = {}

--- 创建带显式类型标记的参数表，不执行 SQL，也不在此编码 JSON 或校验数据库类型。
--- 标记保存在普通字段中；跨服务序列化即使不保留元表，也能由接收方识别。
---@param kind 'null'|'text'|'json'|'bytes'
---@param value? SqlXJsonValue
---@param type_name? SqlXNullType|string
---@overload fun(kind: 'null', value: nil, type_name?: SqlXNullType|string): SqlXNullParam
---@overload fun(kind: 'text', value: string): SqlXTextParam
---@overload fun(kind: 'bytes', value: string): SqlXBytesParam
---@overload fun(kind: 'json', value: SqlXJsonValue): SqlXJsonParam
---@return SqlXNullParam|SqlXTextParam|SqlXJsonParam|SqlXBytesParam
local function typed_param(kind, value, type_name)
    return setmetatable({
        __sqlx_param = kind,
        value = value,
        type = type_name,
    }, sqlx_param_metatable)
end

--- 构造带 SQL 类型的 NULL 绑定参数，不建立连接或执行 SQL。
--- 裸 nil/json.null 参数默认按 TEXT NULL 绑定；PG 的非文本 NULL 应使用匹配的类型。
--- sqlx.null("jsonb") 是 SQL NULL，与 sqlx.json(nil) 表示的 JSON null 不同。
--- 类型是否受支持在提交 SQL 时校验；本函数只创建参数包装表。
--- 用法：db:query("SELECT $1::BIGINT AS value", sqlx.null("int8"))。
---@param type_name? SqlXNullType|string 默认 text，支持类型别名及大小写归一化。
---@return SqlXNullParam param 可传给查询、执行或事务接口的 NULL 参数。
function M.null(type_name)
    return typed_param("null", nil, type_name or "text")
end

--- 显式将 Lua 字符串绑定为 SQL TEXT，不根据 { / [ 前缀猜测 JSON。
--- 普通 Lua 字符串已有相同绑定行为；此包装器用于明确表达文本语义。
--- 非 string 参数立即抛出错误；UTF-8 合法性在提交 SQL 时校验，二进制请用 bytes。
--- PG 日期/UUID 等文本输入可用 $1::TEXT::DATE 或 $1::TEXT::UUID 让数据库解析。
---@param value string UTF-8 文本内容，不是 SQL 片段，不会直接拼接进 SQL。
---@return SqlXTextParam param TEXT 绑定参数。
function M.text(value)
    assert(type(value) == "string", "sqlx.text value must be a string")
    return typed_param("text", value)
end

--- 将 Lua 值显式绑定为 JSON/JSONB，支持对象、数组及字符串/数字/布尔等标量。
--- 字符串会编码成 JSON 字符串值，不作为现成 JSON 文档解析；已有 JSON 文本请先解码。
--- nil/json.null 编码成 JSON null；需要数据库 SQL NULL 时使用 sqlx.null("jsonb")。
--- table 在提交 SQL 时才编码，不会在构造包装器时深拷贝；编码失败由执行接口报告。
--- 普通 table 参数默认也按 JSON 处理；本函数尤其适用于标量和 JSON null。
--- SQLx 直接构造 JSON 值后编码一次；空 table 为 []，对象键限 UTF-8 字符串/整数，数值必须有限。
--- table 最多嵌套 64 层；循环表、非法键值在提交阶段报错，不会默默丢弃。
--- 用法：db:query("SELECT $1::JSONB AS value", sqlx.json({ enabled = true }))。
---@param value SqlXJsonValue 待编码的 Lua 值；数值需能表示为合法 JSON 数字。
---@return SqlXJsonParam param JSON/JSONB 绑定参数。
function M.json(value)
    return typed_param("json", value)
end

--- 将 Lua 字符串作为原始二进制绑定，PG 对应 BYTEA，不做 UTF-8 或 JSON 解码。
--- 可以包含 NUL 和非法 UTF-8 字节；非 string 参数立即抛出 Lua 错误。
--- 字符串内容就是实际字节，不会自动解析十六进制文本或 Base64。
--- 用法：db:query("SELECT $1::BYTEA AS value", sqlx.bytes(string.char(0, 255)))。
---@param value string 原始字节串，可为空。
---@return SqlXBytesParam param 二进制绑定参数。
function M.bytes(value)
    assert(type(value) == "string", "sqlx.bytes value must be a string")
    return typed_param("bytes", value)
end

--- 构造 PostgreSQL 一维数组参数；普通 Lua table 默认是 JSON，不能代替此包装器。
--- 支持的元素类型见 SqlXArrayType；元素类型和各元素的合法性在提交 SQL 时校验。
--- {} 表示空数组；数组中某个 SQL NULL 元素用 json.null 占位，不能用 nil 制造空洞。
--- 构造时只检查参数的 Lua 类型，不深拷贝 values；提交之前对 values 的修改会生效。
--- 普通标记字段可跨 Moon 服务序列化；MySQL/SQLite 不支持这些 PG 数组参数。
--- 用法：db:query("SELECT $1::BIGINT[] AS value", sqlx.array("int8", {1, json.null, 3}))。
---@param element_type SqlXArrayType|string 元素类型，允许 [] 后缀，如 int8[]。
---@param values SqlXArrayValue[] 无空洞序列；NULL 元素使用 json.null，不使用 nil。
---@return SqlXArrayParam param PG 数组绑定参数。
function M.array(element_type, values)
    assert(type(element_type) == "string", "sqlx.array element_type must be a string")
    assert(type(values) == "table", "sqlx.array values must be a table")
    return setmetatable({
        -- Keep a plain field as well as the metatable marker. Moon's inter-service
        -- serializer does not preserve metatables, so sqlxdriver can restore the
        -- typed array on the receiving service.
        __sqlx_array = true,
        type = element_type,
        values = values,
    }, pg_array_metatable)
end

--- 尝试建立一个新的命名数据库连接，并挂起当前协程等待连接结果。
--- 每次调用都尝试新建连接，不按 name 复用；复用请用 find_connection。
--- 新连接成功建立后才替换同名注册项，并触发旧连接排空关闭；不会等待旧连接关闭完毕。
--- 连接名在同一 Moon 进程内共享；其他服务可按名获取同一个底层连接和 FIFO 队列。
--- 默认：连接超时 5000ms、执行超时 30000ms、查询上限 100000 行、等待队列容量 100。
--- request_timeout 只计算请求开始执行后的耗时，不含排队；队列满时提交请求返回 BUSY。
--- 成功返回 db；连接/参数错误返回 nil, err，不在本函数中重试。
--- 已有 db 内部重建失败后按 250/500/1000/.../5000ms 退避，冷却请求直接失败且不提交 SQL。
--- 显式 try_connect 每次仍只尝试一次，不共享全局退避；调用方主动重复建连需自行控制频率。
---@async
---@param database_url string mysql://、postgres://、postgresql:// 或 sqlite: URL。
---@param name string 进程内共享连接名；不同独立连接应使用不同名称。
---@param options? SqlXConnectOptions|integer 配置表，或仅指定连接超时毫秒数；省略项使用默认值。
---@return SqlX? connection 成功时可用的连接对象；失败为 nil。
---@return SqlXError? err 失败时的错误表；成功无第二个返回值。
function M.try_connect(database_url, name, options)
    local connect_timeout
    local request_timeout
    local max_rows
    local queue_capacity
    local reconnect_initial_delay
    local reconnect_max_delay
    local reconnect_log_interval
    if type(options) == "table" then
        connect_timeout = options.connect_timeout
        request_timeout = options.request_timeout
        max_rows = options.max_rows
        queue_capacity = options.queue_capacity
        reconnect_initial_delay = options.reconnect_initial_delay
        reconnect_max_delay = options.reconnect_max_delay
        reconnect_log_interval = options.reconnect_log_interval
    else
        connect_timeout = options
    end

    local ok, session, guard = pcall(c.connect,
        protocol_type,
        moon.id,
        moon.next_sequence(),
        database_url,
        name,
        connect_timeout,
        request_timeout,
        max_rows,
        queue_capacity,
        reconnect_initial_delay,
        reconnect_max_delay,
        reconnect_log_interval)
    if not ok then
        return nil, failure("ERROR", session)
    end
    local res = wait_result(session, guard)
    if type(res) == "table" and res.kind then
        return nil, res
    end
    return setmetatable({ obj = res }, { __index = M })
end

--- 建立命名连接并等待成功；这是 try_connect 的抛错版本，适合初始化时必须连接成功的场景。
--- 连接失败会抛出包含错误消息的 Lua 错误；需要 kind/sqlstate 等结构化信息时使用 try_connect。
--- 参数默认值、同名替换和 FIFO 行为与 try_connect 相同，不会因为名字相同而复用连接。
--- SQLite 数据库文件不存在时尝试创建；本函数不会创建 PostgreSQL/MySQL 数据库。
--- 用法：local db = sqlx.connect(database_url, "main", { request_timeout = 30000 })。
---@async
---@nodiscard
---@param database_url string 数据库 URL。
---@param name string 进程内共享连接名。
---@param options? SqlXConnectOptions|integer 配置表或连接超时毫秒数。
---@return SqlX connection 已建立的连接；失败抛出 Lua 错误，不返回 nil。
function M.connect(database_url, name, options)
    local connection, err = M.try_connect(database_url, name, options)
    if not connection then
        error(string.format("connect database failed: %s", err.message))
    end
    return connection
end

--- 按名查找本进程已有的可用连接；立即返回，不发起网络连接，也不等待连接建立。
--- 命中时创建新的 Lua 包装对象，但共享相同底层连接、选项及 FIFO 队列，不增加并发度。
--- 任意共享对象调用 close 都会关闭同一个底层连接，并影响其他持有者。
--- 注册表不负责永久保活；需要持续使用时应持有返回对象，而不是只记住连接名。
---@nodiscard
---@param name string 与 connect/try_connect 使用的连接名一致。
---@return SqlX? connection 不存在、已释放或已进入关闭流程时返回 nil。
function M.find_connection(name)
    local obj = c.find_connection(name)
    if type(obj) == "table" and obj.kind then
        error(obj.message, 2)
    end
    if obj == nil then
        return nil
    end
    return setmetatable({ obj = obj }, { __index = M })
end

--- 获取本进程注册连接的请求数快照，立即返回，不访问数据库。
--- 每个值包含已入队等待及正在执行的请求，不是累计执行次数，也不是连接池大小。
--- 并发请求和连接关闭会使快照立即过时；不能据此判断所有数据库工作已经完成。
--- 不包含 lrust_sqldriver 服务尚未提交到 Rust 的 Lua 队列；driver 状态请查客户端 stats。
--- 用法：local pending = sqlx.stats()["main"]；nil 表示快照中没有该注册项。
---@nodiscard
---@return SqlXStats stats 连接名到未完成请求数的映射；无注册连接时为空表。
function M.stats()
    return c.stats()
end

--- 获取本进程 SQLx 响应等待状态；只读快照，不访问数据库，不改变任何容量限制。
--- waiting 为仍在等待完成的请求数，ready 为已完成但 Lua 尚未解码的结果数。
--- 等待中断/服务销毁会释放响应；丢弃响应不会取消、回滚或重放已接收的 SQL。
---@return {waiting: integer, ready: integer} stats
function M.response_stats()
    return c.response_stats()
end

--- 发起优雅关闭：停止接受新请求，等待已接收请求处理完，再关闭底层连接。
--- 挂起当前协程直到关闭完成；包含先前 execute 等不等待接口已成功提交的请求。
--- 关闭的是共享连接，而不只是当前包装对象；其他持有者之后提交请求也会得到 CLOSED。
--- 成功后清空当前 obj；本对象再次 close 立即返回 true，其他共享对象重复关闭也安全。
--- 关闭不会撤销已经提交的 SQL；不能用它代替事务回滚。
--- 排空后的正常协议关闭最多等待 1 秒，超时即丢弃底层连接；故障连接已在返回错误前丢弃。
--- 失败或等待中断返回 nil, err，保留当前句柄供再次等待关闭；不保证底层仍能接受请求。
--- close 没有单独的总等待超时；等待排空可能超过单个请求的 request_timeout。
---@async
---@return boolean? closed 成功返回 true，失败返回 nil。
---@return SqlXError? err 关闭失败或等待中断的错误；成功无第二个返回值。
function M:close()
    if self.obj == nil then
        return true
    end
    local session, guard = self.obj:close(protocol_type, moon.id, moon.next_sequence())
    local res = wait_result(session, guard)
    if type(res) == "table" and res.kind then
        return nil, res
    end
    self.obj = nil
    return true
end

--- 提交单条参数化 SQL，不挂起等待、不返回成功状态或执行结果。
--- 适合无需当前调用确认结果的写入；调用返回不表示已执行成功，甚至可能因 BUSY/CLOSED 未入队。
--- 提交阶段错误由 Lua 记录，已入队请求的数据库错误/超时由 Rust 记录；不自动重放。
--- 与同一连接的其他请求共享 FIFO；需要确认写入成功用 execute_wait，需要 RETURNING 行用 query。
---@param sql string 支持位置参数的单条 SQL，例如 PG 的 $1、$2。
---@param ... SqlXParam 与占位符对应的绑定参数，保留尾部 nil；值不会直接拼接进 SQL。
function M:execute(sql, ...)
    log_error(invoke(self, "execute", 0, sql, ...))
end

--- 执行单条参数化 SQL，等待完成并返回执行元数据，不返回 SELECT/RETURNING 的查询行。
--- 适用于 INSERT/UPDATE/DELETE 等需要确认结果的操作；成功可读取 result.rows_affected。
--- MySQL/SQLite 可能提供 last_insert_id；PG 自增值应改用 query 执行 INSERT ... RETURNING。
--- 先检查 result.kind：存在表示失败，否则为成功元数据；不是 nil, err 双返回值。
--- TIMEOUT/SOCKET 不代表写入一定没有提交；接口不会自动重放，需要业务处理结果不确定性。
---@async
---@nodiscard
---@param sql string 单条 SQL；PG 使用 $1、$2 等位置参数。
---@param ... SqlXParam 与占位符对应的绑定参数，保留尾部 nil。
---@return SqlXWriteResult result 成功为执行元数据，失败为带 kind/message 的错误表。
function M:execute_wait(sql, ...)
    return wait_result(invoke(self, "execute", moon.next_sequence(), sql, ...))
end

--- 一次提交并等待执行原始 SQL 文本，可包含以分号分隔的多条语句。
--- 不接受绑定参数，也不返回各语句的查询行；成功返回累计 rows_affected 等执行元数据。
--- 本接口不主动建立事务，不承诺跨语句原子性；需要原子执行请用 transaction。
--- 整个 batch 共用一次 request_timeout，不是每条语句单独计时；失败不保证所有语句均未生效。
--- 仅用于可信 SQL（例如固定的建表脚本），不要拼接未转义的业务输入。
---@async
---@nodiscard
---@param sql string 原始 SQL 文本；需要参数绑定时使用单条执行或事务接口。
---@return SqlXWriteResult result 成功为执行元数据，失败为带 kind/message 的错误表。
function M:batch(sql)
    return wait_result(invoke(self, "batch", moon.next_sequence(), sql))
end

--- batch 的不等待版本：提交原始多语句 SQL 后立即返回，不提供成功确认或查询行。
--- 不接受绑定参数、不承诺事务原子性；可信 SQL、超时和部分执行限制与 batch 相同。
--- 提交错误和后续执行错误仅记录日志；需要确认完成时使用 batch。
---@param sql string 可包含多条语句的可信原始 SQL 文本。
function M:execute_batch(sql)
    log_error(invoke(self, "batch", 0, sql))
end

--- 执行单条参数化 SQL，等待并收集全部查询行；也支持 INSERT/UPDATE/DELETE ... RETURNING。
--- 成功直接返回行数组，不套 data 字段；没有记录时为 {}。失败返回 SqlXError，先检查 result.kind。
--- 每行以列名为键；重复列名会报解码错误，应使用 AS 区分；SQL NULL 字段保留为 json.null。
--- 查询超过 max_rows 时返回错误，不返回截断的部分行；大结果集应自行分页。
--- 超限会丢弃未读完结果的连接；下一条请求重连，会话设置及临时表不会保留。
--- PG 普通整数/浮点数/字符串参数分别按 INT8/FLOAT8/TEXT 绑定，日期文本可用 $1::TEXT::DATE。
--- 当前 NUMERIC/DECIMAL/MONEY 非 NULL 值尚不支持原生解码，需显式 ::TEXT；并未自动转成字符串。
--- SUM(bigint) 等返回 NUMERIC 的表达式也受此限制；确认不溢出时可对结果显式 ::BIGINT。
--- 超时、解码失败或行数超限不等于 SQL 没有生效，尤其不要自动重放带写入的 RETURNING 查询。
--- 用法：local rows = db:query("SELECT id FROM users WHERE id = $1", 42)。
---@async
---@nodiscard
---@param sql string 单条 SQL，PG 使用 $1、$2 等位置参数。
---@param ... SqlXParam 与占位符对应的绑定参数，保留尾部 nil；不是拼接到 SQL 的字符串。
---@return SqlXQueryResult result 成功为行数组（可能为空），失败为带 kind/message 的错误表。
function M:query(sql, ...)
    return wait_result(invoke(self, "query", moon.next_sequence(), sql, ...))
end

--- 校验完整事务列表，并把每条 SQL 及绑定参数加入原生事务构建器；尚不提交数据库。
--- 外层必须是无空洞序列；每条语句首项必须为 SQL 字符串，n 可保留尾部 nil。
--- 任意语句构建失败会抛出错误，由公开事务接口捕获；不会提交已构建的部分列表。
---@param queries SqlXStatement[] 无空洞的语句序列。
---@return SqlXNativeTransaction transaction 待一次性提交的事务构建器。
local function make_transaction(queries)
    assert(type(queries) == "table", "SQLx transaction queries must be a table")
    local count = #queries
    local keys = 0
    for key in pairs(queries) do
        assert(math.type(key) == "integer" and key >= 1 and key <= count,
            "SQLx transaction queries must be a sequence without holes")
        keys = keys + 1
    end
    assert(keys == count, "SQLx transaction queries must be a sequence without holes")
    local trans = c.make_transaction()
    if type(trans) == "table" and trans.kind then error(trans.message, 0) end
    for i = 1, count do
        local query = queries[i]
        assert(type(query) == "table" and type(query[1]) == "string",
            "SQLx transaction statement must start with an SQL string")
        local n = query.n or #query
        assert(math.type(n) == "integer" and n >= 1,
            "SQLx transaction statement length must be a positive integer")
        local result = trans:push(table.unpack(query, 1, n))
        if type(result) == "table" and result.kind then error(result.message, 0) end
    end
    return trans
end

--- 将语句序列作为一个队列请求，在同一连接的一个数据库事务中依次执行并等待提交。
--- 由接口管理开始/提交/回滚，不要在列表中手动混入 BEGIN/COMMIT/ROLLBACK。
--- SQL 执行失败会回滚该事务；超时/断线，尤其发生在提交阶段时，最终结果可能不确定。
--- 普通语句错误会等待显式回滚完成再返回；回滚也受本次 request_timeout 约束。
--- COMMIT/ROLLBACK 自身失败时丢弃连接，保留错误元数据；下一条请求重连。
--- 每条语句独立绑定参数，PG 的 $1 从该语句重新编号；整个事务共用一次 request_timeout。
--- 语句格式为 {sql, param1, ...}；有尾部 nil 时使用 table.pack(sql, ...) 保留总长度 n。
--- 外层列表必须无空洞；构建失败不提交任何语句。允许空列表，成功的 rows_affected 为 0。
--- 成功返回所有语句累计 rows_affected，不返回逐条结果或 RETURNING 行；先检查 result.kind。
--- 事务只包含本次列表，不会自动包含前后单独发出的 query/execute 请求，也不会自动重放。
---@async
---@nodiscard
---@param querys SqlXStatement[] 无空洞的事务语句列表；单条语句也需放在外层列表中。
---@return SqlXTransactionResponse result 成功为事务元数据，失败为带 kind/message 的错误表。
function M:transaction(querys)
    local ok, trans = pcall(make_transaction, querys)
    if not ok then
        return failure("ERROR", trans)
    end
    return wait_result(invoke(self, "transaction", moon.next_sequence(), trans))
end

--- transaction 的不等待版本：先完整校验/构建，再一次性提交事务，不等待提交结果。
--- 语句格式、参数编号、原子执行及总执行超时与 transaction 相同；不会逐条独立入队。
--- 构建/入队错误和后续数据库错误仅记录日志，不返回成功状态、影响行数或查询行。
--- 调用返回不代表事务已经提交成功；需要确认业务写入时使用 transaction，不自动重放。
---@param querys SqlXStatement[] 无空洞的事务语句序列，可为空；尾部 nil 用 table.pack 保留。
function M:execute_transaction(querys)
    local ok, trans = pcall(make_transaction, querys)
    if not ok then
        log_error(failure("ERROR", trans))
        return
    end
    log_error(invoke(self, "transaction", 0, trans))
end

return M
