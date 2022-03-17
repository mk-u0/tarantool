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

g.test_mvcc_different_select_behavior = function()
    g.server:exec(function()
        local t = require('luatest')
        local fiber = require('fiber')

        local s = box.schema.space.create('test')
        s:create_index('primary')

        local f = fiber.create(function()
            fiber.self():set_joinable(true)
            s:insert{1}
        end)

        local res1 = nil
        local res2 = nil
        local res3 = nil

        t.assert_equals(s:select(), {})
        t.assert_equals(s:select(1), {})
        t.assert_equals(s:count(), 0)
        box.begin()
        res1 = s:select()
        res2 = s:select(1)
        res3 = s:count()
        box.commit()
        t.assert_equals(res1, {})
        t.assert_equals(res2, {})
        t.assert_equals(res3, 0)

        -- for RW transactions prepared statements are visible
        box.begin()
        s:replace{2}
        res1 = s:select()
        res2 = s:select(1)
        res3 = s:count()
        box.commit()
        t.assert_equals(res1, {{1}, {2}})
        t.assert_equals(res2, {{1}})
        t.assert_equals(res3, 2)

        f:join()

        t.assert_equals(s:select(), {{1}, {2}})
        t.assert_equals(s:select(1), {{1}})
        t.assert_equals(s:count(), 2)

        s:truncate()

        -- If RO transaction becomes RW - it can be aborted
        f = fiber.create(function()
            fiber.self():set_joinable(true)
            s:insert{1}
        end)

        box.begin()
        res1 = s:select()
        res2 = s:select(1)
        res3 = s:count()
        s:replace{2}
        t.assert_error_msg_content_equals(
            "Transaction has been aborted by conflict",
            function() box.commit() end)
        t.assert_equals(res1, {})
        t.assert_equals(res2, {})
        t.assert_equals(res3, 0)

        f:join()

        -- Similar conflict but with skipped detele
        f = fiber.create(function()
            fiber.self():set_joinable(true)
            s:delete{1}
        end)

        box.begin()
        res1 = s:select()
        res2 = s:select(1)
        res3 = s:count()
        s:replace{2}
        t.assert_error_msg_content_equals(
            "Transaction has been aborted by conflict",
            function() box.commit() end)
        t.assert_equals(res1, {{1}})
        t.assert_equals(res2, {{1}})
        t.assert_equals(res3, 1)

        s:drop()
    end)
end
