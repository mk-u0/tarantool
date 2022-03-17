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

g.test_mvcc_wrong_space_count = function()
    g.server:exec(function()
        local t = require('luatest')
        local fiber = require('fiber')

        local s = box.schema.space.create('test')
        s:create_index('primary')

        local f1 = fiber.create(function()
            fiber.self():set_joinable(true)
            s:insert{1}
        end)

        t.assert_equals(s:select(), {})
        t.assert_equals(s:count(), 0)

        local f2_select = nil
        local f2_count = nil
        local f2 = fiber.create(function()
            fiber.self():set_joinable(true)
            box.begin()
            s:insert{2}
            f2_select = s:select()
            f2_count = s:count()
            box.commit()
        end)

        -- RW transaction must see prepared statements
        t.assert_equals(f2_select, {{1}, {2}})
        t.assert_equals(f2_count, 2)

        t.assert_equals(s:select(), {})
        t.assert_equals(s:count(), 0)

        f1:join()
        f2:join()

        t.assert_equals(s:select(), {{1}, {2}})
        t.assert_equals(s:count(), 2)

        s:drop()
    end)
end
