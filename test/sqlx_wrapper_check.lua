-- Standalone wrapper regression checks: no Moon process or database needed.
-- From the Moon workspace: premake5 --file=ext/lrust/test/sqlx_wrapper_check.lua check_sqlx
local root = _SCRIPT_DIR and (_SCRIPT_DIR .. "/../") or "ext/lrust/"
local function compile_file(filename, environment)
    local file = assert(io.open(filename, "rb"))
    local source = file:read("*a")
    file:close()
    return assert(load(source, "@" .. filename, "t", environment or _G))
end
local pending, calls, logs = {}, {}, {}
local sequence, break_wait = 0, false
local registered
local moon = {
    id = 42,
    next_sequence = function() sequence = sequence + 1; return sequence end,
    register_protocol = function(p) registered = p end,
    error = function(message) logs[#logs + 1] = message end,
    wait = function(session)
        assert(type(session) == "number" and session > 0, "invalid wait session")
        if break_wait then break_wait = false; return false, "BREAK" end
        local res = pending[session]
        pending[session] = nil
        assert(res ~= nil, "missing mock response")
        return res
    end,
}
local native = {}
for _, method in ipairs({ "query", "execute", "transaction", "batch" }) do
    native[method] = function(_, owner, session, ...)
        assert(owner == moon.id)
        calls[#calls + 1] = { method = method, args = table.pack(...) }
        if method == "query" and (...) == "invalid" then error("invalid parameter") end
        pending[session] = method == "query" and {} or { rows_affected = 2 }
        return session
    end
end
function native:close(ptype, owner, session)
    assert(ptype == 23 and owner == moon.id)
    pending[session] = true
    return session
end
local c = {
    connect = function(ptype, owner, session, _, _, connect_timeout)
        assert(ptype == 23 and owner == moon.id)
        if connect_timeout == -1 then error("invalid timeout") end
        pending[session] = native
        return session
    end,
    find_connection = function(name) return name == "exists" and native or nil end,
    decode = function(token, owner) assert(owner == moon.id); return token end,
    make_transaction = function()
        local trans = { statements = {} }
        function trans:push(...) self.statements[#self.statements + 1] = table.pack(...) end
        return trans
    end,
}
local env = setmetatable({ require = function(name)
    if name == "moon" then return moon end
    assert(name == "rust.sqlx", name)
    return c
end }, { __index = _G })
local sqlx = compile_file(root .. "lualib/sqlx.lua", env)()
assert(registered.PTYPE == 23 and registered.unpack(99) == 99)
assert(sqlx.find_connection("missing") == nil)
local db, err = sqlx.try_connect("postgresql://mock", "exists")
assert(db and not err)
assert(db:query("SELECT $1, $2", 1, nil).kind == nil)
assert(calls[#calls].args.n == 3 and calls[#calls].args[3] == nil)
assert(db:query("invalid").kind == "ERROR")
assert(db:execute_wait("UPDATE mock").rows_affected == 2)
assert(calls[#calls].method == "execute")
assert(db:batch("SELECT 1; SELECT 2").rows_affected == 2)
local statements = { table.pack("SELECT $1, $2", 1, nil) }
assert(db:transaction(statements).rows_affected == 2)
local trans = calls[#calls].args[1]
assert(trans.statements[1].n == 3 and trans.statements[1][3] == nil)
local before = #calls
assert(db:transaction({ [1] = { "SELECT 1" }, [3] = { "SELECT 3" } }).kind == "ERROR")
assert(#calls == before, "sparse transaction must not be partially submitted")
local bad, baderr = sqlx.try_connect("mock", "bad", -1)
assert(bad == nil and baderr.kind == "ERROR")
break_wait = true
bad, baderr = sqlx.try_connect("mock", "interrupted")
assert(bad == nil and baderr.message == "BREAK")
break_wait = true
local closed, closeerr = db:close()
assert(closed == nil and closeerr.kind == "ERROR" and db.obj ~= nil)
assert(db:close() == true and db:close() == true)
assert(db:query("SELECT 1").kind == "CLOSED")
db:execute("UPDATE mock")
assert(#logs == 1)
assert(sqlx.null("int8").__sqlx_param == "null")
assert(sqlx.bytes("\255\0").value == "\255\0")
assert(sqlx.json(false).value == false)
compile_file(root .. "../../service/lrust_sqldriver.lua")
compile_file(root .. "../../lualib/lrust_sqldriver/client.lua")
compile_file(root .. "../../example/example_lrust_sqldriver.lua")
print("SQLx wrapper regression checks passed (" .. _VERSION .. "); service/client/example syntax OK")

-- Driver split regressions: all imports below are sandboxed mocks, not Moon/Rust.
local workspace = root .. "../../"
local driver_service_path = workspace .. "service/lrust_sqldriver.lua"
local driver_client_path = workspace .. "lualib/lrust_sqldriver/client.lua"
local search_paths = workspace .. "lualib/?.lua;" .. workspace .. "service/?.lua"
local function normalized_path(path) return (path:gsub("\\", "/")) end
assert(normalized_path(assert(package.searchpath("lrust_sqldriver", search_paths))) == normalized_path(driver_service_path))
assert(normalized_path(assert(package.searchpath("lrust_sqldriver.client", search_paths))) == normalized_path(driver_client_path))
local function make_client_loader()
    local outgoing, cache = {}, {}
    local reply = { marker = "unchanged RPC result" }
    local mock_moon = {
        call = function(...)
            outgoing[#outgoing + 1] = { mode = "call", args = table.pack(...) }
            return reply
        end,
        send = function(...)
            outgoing[#outgoing + 1] = { mode = "send", args = table.pack(...) }
        end,
    }
    local client_env
    local function import(name)
        if cache[name] then return cache[name] end
        if name == "moon" then return mock_moon end
        if name == "buffer" then return { unpack = function(value) return value end } end
        if name == "json" then return { concat = table.concat } end
        assert(name == "lrust_sqldriver.client", "client must not load service dependencies: " .. name)
        local path = driver_client_path
        cache[name] = compile_file(path, client_env)(name, path)
        return cache[name]
    end
    client_env = setmetatable({ require = import }, { __index = _G })
    return import, outgoing, reply
end

-- Importing the client is cached and never loads the service or ext.sqlx.
local import, outgoing, reply = make_client_loader()
local api = import("lrust_sqldriver.client")
assert(import("lrust_sqldriver.client") == api)
assert(#outgoing == 0, "importing client API must not send requests")
assert(api.pipe == api.transaction and api.execute_pipe == api.execute_transaction)

-- The service rejects missing configuration/old require before loading dependencies.
local dependency_loads = 0
local service_entry = compile_file(driver_service_path, setmetatable({ require = function(name)
    dependency_loads = dependency_loads + 1
    error("invalid service configuration must not load dependencies: " .. name)
end }, { __index = _G }))
local invalid_configs = table.pack(nil, "lrust_sqldriver", {}, false)
for i = 1, invalid_configs.n do
    local ok, message = pcall(service_entry, invalid_configs[i])
    assert(not ok and tostring(message):find("service requires configuration", 1, true))
end
assert(dependency_loads == 0)

-- Verify every query/write/transaction routing variant and trailing nil binding.
local variants = {
    { "query", "query", true },
    { "execute", "execute", false },
    { "execute_wait", "execute", true },
    { "query_params", "query_params", true, true },
    { "execute_params", "execute_params", false, true },
    { "execute_params_wait", "execute_params", true, true },
    { "batch", "batch", true },
    { "execute_batch", "batch", false, false, { "" } },
    { "transaction", "transaction", true },
    { "execute_transaction", "transaction", false, false, { "", "_on" } },
}
local covered = {}
for _, spec in ipairs(variants) do
    for _, suffix in ipairs(spec[5] or { "", "_on", "_any" }) do
        local name = spec[1] .. suffix
        local is_transaction = spec[2] == "transaction"
        local args = table.pack(9001)
        local function append(value) args.n = args.n + 1; args[args.n] = value end
        if suffix == "_on" then append("player:7") end
        append(is_transaction and { table.pack("SELECT $1::TEXT", nil) } or "SELECT $1")
        if spec[4] then append(7); append(nil) end
        local previous = #outgoing
        local result = api[name](table.unpack(args, 1, args.n))
        assert(#outgoing == previous + 1, name .. ": expected exactly one request")
        local sent = outgoing[#outgoing]
        assert(sent.mode == (spec[3] and "call" or "send"), name)
        assert(sent.args[1] == "lua" and sent.args[2] == 9001 and sent.args[3] == spec[2], name)
        local affinity = 1
        if suffix == "_any" then affinity = false
        elseif suffix == "_on" then affinity = "player:7" end
        assert(sent.args[4] == affinity, name .. ": wrong affinity")
        if spec[3] then assert(result == reply, name .. ": response was changed")
        else assert(result == nil, name .. ": fire-and-forget returned a result") end
        if is_transaction then
            assert(sent.args[5][1][1] == "SELECT $1::TEXT")
            assert(sent.args[5][1].n == 2 and sent.args[5][1][2] == nil)
        else assert(sent.args[5] == "SELECT $1", name) end
        if spec[4] then
            assert(sent.args.n == 6 and sent.args[6].n == 2)
            assert(sent.args[6][1] == 7 and sent.args[6][2] == nil, name .. ": trailing nil lost")
        end
        covered[name] = true
    end
end
api.query(9001, { "SELECT ", "42" }, 99)
assert(outgoing[#outgoing].args[4] == 99 and outgoing[#outgoing].args[5] == "SELECT 42")
assert(api.null("int8").type == "int8" and api.null().__sqlx_param == "null")
assert(api.text("{literal}").value == "{literal}")
assert(api.json(false).value == false)
assert(api.bytes(string.char(0, 255)).value == string.char(0, 255))
local array = api.array("int8", { 1, 2 })
assert(array.__sqlx_array and array.type == "int8" and array.values[2] == 2)
for _, name in ipairs({ "len", "stats" }) do
    assert(api[name](9001) == reply)
    assert(outgoing[#outgoing].args[3] == name and outgoing[#outgoing].mode == "call")
    covered[name] = true
end
api.save_then_quit(9001)
assert(outgoing[#outgoing].args[3] == "save_then_quit" and outgoing[#outgoing].mode == "send")
covered.save_then_quit = true
for _, name in ipairs({ "array", "null", "text", "json", "bytes", "pipe", "execute_pipe" }) do covered[name] = true end
local exports = 0
for name, fn in pairs(api) do
    assert(type(fn) == "function" and covered[name], "uncovered client export: " .. name)
    exports = exports + 1
end
assert(exports == 37, "unexpected public API changes")
local previous = #outgoing
assert(not pcall(api.query_on, 9001, nil, "SELECT 1"))
assert(not pcall(api.transaction, 9001, { [1] = { "SELECT 1" }, [3] = { "SELECT 3" } }))
assert(#outgoing == previous, "invalid client request was partially sent")

-- Execute the actual service entry with mocked SQLx and synchronous workers.
-- This tests initialization, dispatch, validation and cleanup, not DB concurrency.
local function load_service(eager_connect)
    local state = { connections = {}, queries = {}, responses = {}, logs = {}, closed = 0 }
    local mock_moon = {
        async = function(fn) fn() end,
        sleep = function() error("unexpected wait in synchronous driver mock") end,
        dispatch = function(protocol, fn) assert(protocol == "lua"); state.dispatch = fn end,
        shutdown = function(fn) state.shutdown = fn end,
        quit = function() state.quit = true end,
        error = function(message) state.logs[#state.logs + 1] = message end,
        response = function(protocol, sender, session, result)
            assert(protocol == "lua")
            state.responses[#state.responses + 1] = { sender = sender, session = session, result = result }
        end,
    }
    local mock_sqlx = {
        array = function(element_type, values)
            return { __sqlx_array = true, type = element_type, values = values }
        end,
        try_connect = function(url, name, options)
            assert(url == "postgres://mock" and options.connect_timeout == 5000)
            local connection = { name = name }
            for _, method in ipairs({ "query", "execute_wait", "batch", "transaction" }) do
                connection[method] = function(self, ...)
                    state.queries[#state.queries + 1] = { name = self.name, method = method, args = table.pack(...) }
                    if method == "query" then return { { value = 42 } } end
                    return { message = "ok", rows_affected = 1 }
                end
            end
            function connection:close() state.closed = state.closed + 1; return true end
            state.connections[#state.connections + 1] = connection
            return connection
        end,
    }
    local service_env = setmetatable({ require = function(name)
        if name == "moon" then return mock_moon end
        if name == "ext.sqlx" then return mock_sqlx end
        if name == "list" then return compile_file(workspace .. "lualib/list.lua")() end
        error("service must not load the client API: " .. name)
    end }, { __index = _G })
    compile_file(driver_service_path, service_env)({
        name = "split_mock", url = "postgres://mock", poolsize = 2, eager_connect = eager_connect,
    })
    assert(type(state.dispatch) == "function" and type(state.shutdown) == "function")
    function state:request(command, affinity, payload, params, session)
        local response_count = #self.responses
        self.dispatch(88, session or 1, command, affinity, payload, params)
        if session == 0 then
            assert(#self.responses == response_count, "fire-and-forget received a response")
            return
        end
        assert(#self.responses == response_count + 1)
        local response = self.responses[#self.responses]
        assert(response.sender == 88 and response.session == (session or 1))
        return response.result
    end
    return state
end
local service = load_service(true)
assert(#service.connections == 2)
assert(service:request("query", 1, "SELECT 42").data[1].value == 42)
assert(service.queries[#service.queries].name == "lrust_sqldriver:split_mock:2")
local service_params = table.pack(7, nil)
assert(not service:request("query_params", 1, "SELECT $1, $2", service_params).code)
local bound = service.queries[#service.queries].args
assert(bound.n == 3 and bound[2] == 7 and bound[3] == nil)
assert(service:request("execute", 1, "UPDATE mock").rows_affected == 1)
assert(service:request("batch", false, "SELECT 1; SELECT 2").rows_affected == 1)
local transaction = service:request("transaction", 1, { table.pack("SELECT $1::TEXT", nil) })
assert(transaction.data == true and transaction.num_queries == 1)
assert(service.queries[#service.queries].args[1][1].n == 2)
service:request("execute", 1, "UPDATE mock", nil, 0)
assert(service:request("len")[1] == 0 and service:request("len")[2] == 0)
assert(service:request("stats").poolsize == 2)
local before_invalid = #service.queries
assert(service:request("transaction", 1, { [1] = { "SELECT 1" }, [3] = { "SELECT 3" } }).code == "INVALID")
assert(service:request("query", {}, "SELECT 1").code == "INVALID")
assert(service:request("unknown", 1).code == "COMMAND")
assert(#service.queries == before_invalid, "invalid service request reached SQLx")
service:request("save_then_quit", nil, nil, nil, 0)
assert(service.quit and service.closed == 2)
assert(service:request("query", 1, "SELECT 1").code == "CLOSED")
service.shutdown()
assert(service.closed == 2, "shutdown closed a connection twice")
local lazy = load_service(false)
assert(#lazy.connections == 0)
assert(lazy:request("query", 1, "SELECT 1").data[1].value == 42)
assert(#lazy.connections == 1)
lazy.shutdown()
assert(lazy.quit and lazy.closed == 1)
print("Driver split regression checks passed: client import, service configuration, 37 exports, routing, dispatch, lazy connect and shutdown")

if newaction then
    newaction { trigger = "check_sqlx", description = "Check SQLx Lua wrappers", execute = function() end }
end
