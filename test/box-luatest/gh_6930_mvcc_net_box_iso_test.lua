local server = require('test.luatest_helpers.server')
local t = require('luatest')

local g = t.group()

g.before_all = function()
    g.server = server:new{
        alias   = 'default',
        box_cfg = {memtx_use_mvcc_engine = true}
    }
    g.server:start()
end

g.after_all = function()
    g.server:drop()
end

g.test_mvcc_netbox_isolation_level_basics = function()
    local t = require('luatest')

    g.server:exec(function()
        local s = box.schema.space.create('test')
        s:create_index('primary')
        box.schema.user.grant('guest', 'read,write', 'space', 'test')
        box.error.injection.set('ERRINJ_WAL_DELAY', true)
        local fiber = require('fiber')
        local f = fiber.create(function()
            fiber.self():set_joinable(true)
            s:insert{1}
        end)
        rawset(_G, 'f', f)
    end)

    local netbox = require('net.box')
    local conn = netbox.connect(g.server.net_box_uri)

    t.assert_equals(conn.space.test:select(), {})
    local strm = conn:new_stream()
    strm:begin()
    t.assert_equals(strm.space.test:select(), {})
    strm:commit()

    local expect0 = {'default', 'read-confirmed', 'best-efford',
                     box.txn_isolation['default'],
                     box.txn_isolation['read-confirmed'],
                     box.txn_isolation['best-efford']}
    for _,level in pairs(expect0) do
        strm:begin{txn_isolation = level}
        t.assert_equals(strm.space.test:select(), {})
        strm:commit()
    end

    local expect0 = {'read-confirmed', 'best-efford',
                box.txn_isolation['read-confirmed'],
                box.txn_isolation['best-efford']}
    for _,level in pairs(expect0) do
        g.server:exec(function(level)
            box.cfg{default_txn_isolation = level}
        end, {level})
        strm:begin()
        t.assert_equals(strm.space.test:select(), {})
        strm:commit()
    end
    g.server:exec(function()
        box.cfg{default_txn_isolation = 'best-efford'}
    end)

    local expect1 = {'read-committed', box.txn_isolation['read-committed']}
    for _,level in pairs(expect1) do
        strm:begin{txn_isolation = level}
        t.assert_equals(strm.space.test:select(), {{1}})
        strm:commit()
    end

    for _,level in pairs(expect1) do
        g.server:exec(function(level)
            box.cfg{default_txn_isolation = level}
        end, {level})
        strm:begin()
        t.assert_equals(strm.space.test:select(), {{1}})
        strm:commit()
        -- default_txn_isolation does not affect autocommit select,
        -- which is always run as read-confirmed
        t.assert_equals(strm.space.test:select(), {})
    end
    g.server:exec(function()
        box.cfg{default_txn_isolation = 'best-efford'}
    end)

    -- With default best-efford isolation RO->RW transaction can be aborted:
    strm:begin()
    t.assert_equals(strm.space.test:select{1}, {})
    strm.space.test:replace{2}
    t.assert_error_msg_content_equals(
        "Transaction has been aborted by conflict",
        function() strm:commit() end)

    -- But using 'read-committed' allows to avoid conflict:
    strm:begin{txn_isolation = 'read-committed'}
    t.assert_equals(strm.space.test:select{1}, {{1}})
    strm.space.test:replace{2}
    g.server:exec(function()
        box.error.injection.set('ERRINJ_WAL_DELAY', false)
    end)
    strm:commit()

    t.assert_equals(strm.space.test:select{}, {{1}, {2}})

    g.server:exec(function()
        f:join()
        box.space.test:drop()
    end)
end
