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

g.test_mvcc_isolation_level_errors = function()
    g.server:exec(function()
        local t = require('luatest')
        t.assert_error_msg_content_equals(
            "Illegal parameters, txn_isolation must be one of box.txn_isolation (keys or values)",
            function() box.begin{txn_isolation = 'avadakedavra'} end)
        t.assert_error_msg_content_equals(
            "Illegal parameters, txn_isolation must be one of box.txn_isolation (keys or values)",
            function() box.cfg{default_txn_isolation = 'avadakedavra'} end)
        t.assert_error_msg_content_equals(
            "Illegal parameters, txn_isolation must be a string or number",
            function() box.begin{txn_isolation = false} end)
        t.assert_error_msg_content_equals(
            "Incorrect value for option 'default_txn_isolation': should be one of types string, number",
            function() box.cfg{default_txn_isolation = false} end)
        t.assert_error_msg_content_equals(
            "Illegal parameters, unknown isolation level",
            function() box.begin{txn_isolation = 8} end)
        t.assert_error_msg_content_equals(
            "Illegal parameters, unknown isolation level",
            function() box.cfg{default_txn_isolation = 8} end)
    end)
end

g.test_mvcc_isolation_level_basics = function()
    g.server:exec(function()
        local t = require('luatest')
        local fiber = require('fiber')

        local s = box.schema.space.create('test')
        s:create_index('primary')

        local f = fiber.create(function()
            fiber.self():set_joinable(true)
            s:insert{1}
        end)

        t.assert_equals(s:select(), {})
        t.assert_equals(s:count(), 0)

        local res1 = nil
        local res2 = nil

        box.begin()
        res1 = s:select()
        res2 = s:count()
        box.commit()
        t.assert_equals(res1, {})
        t.assert_equals(res2, 0)

        local expect0 = {'default', 'read-confirmed', 'best-efford',
                         box.txn_isolation['default'],
                         box.txn_isolation['read-confirmed'],
                         box.txn_isolation['best-efford']}

        for _,level in pairs(expect0) do
            box.begin{txn_isolation = level}
            res1 = s:select()
            res2 = s:count()
            box.commit()
            t.assert_equals(res1, {})
            t.assert_equals(res2, 0)
        end

        expect0 = {'read-confirmed', 'best-efford',
                    box.txn_isolation['read-confirmed'],
                    box.txn_isolation['best-efford']}

        for _,level in pairs(expect0) do
            box.cfg{default_txn_isolation = level}
            box.begin{}
            res1 = s:select()
            res2 = s:count()
            box.commit()
            t.assert_equals(res1, {})
            t.assert_equals(res2, 0)
            box.cfg{default_txn_isolation = 'best-efford'}
        end

        local expect1 = {'read-committed', box.txn_isolation['read-committed']}

        for _,level in pairs(expect1) do
            box.begin{txn_isolation = level}
            res1 = s:select()
            res2 = s:count()
            box.commit()
            t.assert_equals(res1, {{1}})
            t.assert_equals(res2, 1)
        end

        for _,level in pairs(expect1) do
            box.cfg{default_txn_isolation = level}
            box.begin{txn_isolation = level}
            res1 = s:select()
            res2 = s:count()
            box.commit()
            t.assert_equals(res1, {{1}})
            t.assert_equals(res2, 1)
            -- default_txn_isolation does not affect autocommit select,
            -- which is always run as read-confirmed
            t.assert_equals(s:select(), {})
            t.assert_equals(s:count(), 0)
            box.cfg{default_txn_isolation = 'best-efford'}
        end

        -- With default best-efford isolation RO->RW transaction can be aborted:
        box.begin()
        res1 = s:select(1)
        s:replace{2}
        t.assert_error_msg_content_equals(
            "Transaction has been aborted by conflict",
            function() box.commit() end)
        t.assert_equals(res1, {})

        -- But using 'read-committed' allows to avoid conflict:
        box.begin{txn_isolation = 'read-committed'}
        res1 = s:select(1)
        s:replace{2}
        box.commit()
        t.assert_equals(res1, {{1}})
        t.assert_equals(s:select{}, {{1}, {2}})

        f:join()
        s:drop()
    end)
end
