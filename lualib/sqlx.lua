---@diagnostic disable: inject-field, undefined-global
local moon = require "moon"
---@type any
local c = require "rust.sqlx"

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
---@field value? SqlXJsonValue Lua JSON 值；nil 表示 JSON null，不是 SQL NULL。

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
---@field request_timeout? integer 单个请求开始执行后的超时，毫秒，默认 30000；不含排队时间。
---@field max_rows? integer 单次 query 最大返回行数，默认 100000；不是字节数限制。
---@field queue_capacity? integer Rust 连接待执行队列容量，默认 100；不含正在执行的请求。

---@alias SqlXErrorKind 'ERROR'|'DB'|'SOCKET'|'TIMEOUT'|'BUSY'|'CLOSED'

---@class SqlXError
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
---@class SqlXNativeTransaction
---@field push fun(self: SqlXNativeTransaction, sql: string, ...: SqlXParam)

---@class SqlXNativeConnection
---@field query fun(self: SqlXNativeConnection, owner: integer, session: integer, sql: string, ...: SqlXParam): integer|SqlXError
---@field execute fun(self: SqlXNativeConnection, owner: integer, session: integer, sql: string, ...: SqlXParam): integer|SqlXError
---@field batch fun(self: SqlXNativeConnection, owner: integer, session: integer, sql: string): integer|SqlXError
---@field transaction fun(self: SqlXNativeConnection, owner: integer, session: integer, transaction: SqlXNativeTransaction): integer|SqlXError
---@field close fun(self: SqlXNativeConnection, protocol: integer, owner: integer, session: integer): integer|SqlXError

--- 模块同时作为连接对象的方法表。每个命名连接串行执行已接收的请求。
---@class SqlX
---@field obj? SqlXNativeConnection 内部连接句柄，请勿直接操作；成功 close 后清空。
local M = {}

---@param kind SqlXErrorKind
---@param message any
---@return SqlXError
local function failure(kind, message)
    return { kind = kind, message = tostring(message) }
end

---@async
---@param session integer|SqlXError 会话 ID，或未入队就产生的错误。
---@return any result 会话协议解码后的动态响应；具体返回类型由公开接口约束。
local function wait_result(session)
    if type(session) == "table" then
        return session
    end
    local res, err = moon.wait(session)
    if res == nil or res == false then
        return failure("ERROR", err or "SQLx wait interrupted")
    end
    return res
end

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
    local ok, res = pcall(obj[method], obj, moon.id, session, ...)
    if not ok then
        return failure("ERROR", res)
    end
    return res
end

---@param res integer|SqlXError
local function log_error(res)
    if type(res) == "table" and res.kind then
        moon.error(string.format("SQLx [%s]: %s", res.kind, res.message))
    end
end

local pg_array_metatable = {
    __sqlx_array = true,
}

local sqlx_param_metatable = {}

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

--- 创建指定 SQL 类型的 NULL 参数；PG 非文本 NULL 建议明确指定类型。
---@param type_name? SqlXNullType|string 默认 text，支持类型别名及大小写归一化。
---@return SqlXNullParam param
function M.null(type_name)
    return typed_param("null", nil, type_name or "text")
end

--- 显式绑定 UTF-8 文本，不根据 { / [ 前缀猜测 JSON。
---@param value string
---@return SqlXTextParam param
function M.text(value)
    assert(type(value) == "string", "sqlx.text value must be a string")
    return typed_param("text", value)
end

--- 将 Lua 值绑定为 JSON/JSONB；字符串表示 JSON 字符串值，不是待解析的 JSON 文档。
---@param value SqlXJsonValue
---@return SqlXJsonParam param
function M.json(value)
    return typed_param("json", value)
end

--- 绑定二进制数据，不进行 UTF-8 转换。
---@param value string
---@return SqlXBytesParam param
function M.bytes(value)
    assert(type(value) == "string", "sqlx.bytes value must be a string")
    return typed_param("bytes", value)
end

--- 创建 PostgreSQL 一维数组参数；原始标记字段可跨 Moon 服务序列化。
---@param element_type SqlXArrayType|string 元素类型，允许 [] 后缀，如 int8[]。
---@param values SqlXArrayValue[] 无空洞序列；NULL 元素使用 json.null，不使用 nil。
---@return SqlXArrayParam param
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

--- 尝试建立命名连接；成功返回连接，失败返回 nil, err。
---@async
---@param database_url string mysql://、postgres://、postgresql:// 或 sqlite: URL。
---@param name string 进程内共享连接名；同名新连接会替换并关闭旧连接。
---@param options? SqlXConnectOptions|integer 配置表，或仅指定连接超时毫秒数。
---@return SqlX? connection
---@return SqlXError? err
function M.try_connect(database_url, name, options)
    local connect_timeout
    local request_timeout
    local max_rows
    local queue_capacity
    if type(options) == "table" then
        connect_timeout = options.connect_timeout
        request_timeout = options.request_timeout
        max_rows = options.max_rows
        queue_capacity = options.queue_capacity
    else
        connect_timeout = options
    end

    local ok, session = pcall(c.connect,
        protocol_type,
        moon.id,
        moon.next_sequence(),
        database_url,
        name,
        connect_timeout,
        request_timeout,
        max_rows,
        queue_capacity)
    if not ok then
        return nil, failure("ERROR", session)
    end
    local res = wait_result(session)
    if type(res) == "table" and res.kind then
        return nil, res
    end
    return setmetatable({ obj = res }, { __index = M })
end

--- 建立命名连接；失败时抛出 Lua 错误。不希望抛出错误时使用 try_connect。
--- SQLite 数据库不存在时自动创建；连接名可供其他服务 find_connection。
---@async
---@nodiscard
---@param database_url string 数据库 URL。
---@param name string 进程内共享连接名。
---@param options? SqlXConnectOptions|integer 配置表或连接超时毫秒数。
---@return SqlX connection
function M.connect(database_url, name, options)
    local connection, err = M.try_connect(database_url, name, options)
    if not connection then
        error(string.format("connect database failed: %s", err.message))
    end
    return connection
end

--- 查找已有连接；返回共享同一执行队列的新 Lua 包装对象，不建立新物理连接。
---@nodiscard
---@param name string 连接名。
---@return SqlX? connection 不存在或已进入关闭流程时返回 nil。
function M.find_connection(name)
    local obj = c.find_connection(name)
    if obj == nil then
        return nil
    end
    return setmetatable({ obj = obj }, { __index = M })
end

--- 获取进程内命名连接的请求数，包含排队和正在执行的请求。
---@nodiscard
---@return SqlXStats stats
function M.stats()
    return c.stats()
end

--- 停止接收新请求，等待已接收请求排空并关闭连接；重复调用安全。
--- 关闭的是共享底层连接，不仅是当前 Lua 包装对象。
---@async
---@return boolean? closed 成功返回 true，失败返回 nil。
---@return SqlXError? err
function M:close()
    if self.obj == nil then
        return true
    end
    local session = self.obj:close(protocol_type, moon.id, moon.next_sequence())
    local res = wait_result(session)
    if type(res) == "table" and res.kind then
        return nil, res
    end
    self.obj = nil
    return true
end

--- 提交单条语句，不等待、不返回执行结果；错误仅记录日志。
---@param sql string 支持位置参数的单条 SQL，例如 PG 的 $1、$2。
---@param ... SqlXParam 绑定参数，保留尾部 nil。
function M:execute(sql, ...)
    log_error(invoke(self, "execute", 0, sql, ...))
end

--- 执行单条语句并等待元数据；错误在 result.kind 中，不返回查询行。
---@async
---@nodiscard
---@param sql string
---@param ... SqlXParam
---@return SqlXWriteResult result
function M:execute_wait(sql, ...)
    return wait_result(invoke(self, "execute", moon.next_sequence(), sql, ...))
end

--- 执行以分号分隔的原始 SQL；不接受绑定参数，也不承诺事务原子性。
---@async
---@nodiscard
---@param sql string
---@return SqlXWriteResult result
function M:batch(sql)
    return wait_result(invoke(self, "batch", moon.next_sequence(), sql))
end

--- batch 的不等待版本；不返回执行结果，错误仅记录日志。
---@param sql string
function M:execute_batch(sql)
    log_error(invoke(self, "batch", 0, sql))
end

--- 查询并等待行数组；也可执行 INSERT ... RETURNING。先检查 result.kind 判断失败。
--- 列名作为行表的键；重复列名需用 SQL 别名区分，SQL NULL 保留为 json.null。
---@async
---@nodiscard
---@param sql string 单条 SQL，PG 使用 $1、$2 等位置参数。
---@param ... SqlXParam
---@return SqlXQueryResult result 成功为行数组（可能为空），失败为 SqlXError。
function M:query(sql, ...)
    return wait_result(invoke(self, "query", moon.next_sequence(), sql, ...))
end

---@param queries SqlXStatement[] 无空洞的语句序列。
---@return SqlXNativeTransaction
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
    for i = 1, count do
        local query = queries[i]
        assert(type(query) == "table" and type(query[1]) == "string",
            "SQLx transaction statement must start with an SQL string")
        local n = query.n or #query
        assert(math.type(n) == "integer" and n >= 1,
            "SQLx transaction statement length must be a positive integer")
        trans:push(table.unpack(query, 1, n))
    end
    return trans
end

--- 在同一个事务中执行语句序列；SQL 失败会回滚，不返回每条语句的查询行。
--- 语句形式为 {sql, param1, ...}；尾部 nil 使用 table.pack(sql, ...) 保留。
--- 超时/断线可能造成提交结果不确定，不会自动重放。
---@async
---@nodiscard
---@param querys SqlXStatement[] 无空洞序列，可为空。
---@return SqlXTransactionResponse result
function M:transaction(querys)
    local ok, trans = pcall(make_transaction, querys)
    if not ok then
        return failure("ERROR", trans)
    end
    return wait_result(invoke(self, "transaction", moon.next_sequence(), trans))
end

--- 提交事务但不等待结果；错误仅记录日志，不自动重放。
---@param querys SqlXStatement[] 无空洞的事务语句序列。
function M:execute_transaction(querys)
    local ok, trans = pcall(make_transaction, querys)
    if not ok then
        log_error(failure("ERROR", trans))
        return
    end
    log_error(invoke(self, "transaction", 0, trans))
end

return M
