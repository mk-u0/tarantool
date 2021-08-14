env = require('test_run')
test_run = env.new()
engine = test_run:get_cfg('engine')

S = box.schema.space.create('S', {engine = engine, format = {{'d', 'double'}}})
_ = S:create_index('pk')

S:insert{1.1}
S:insert{4.5}
S:insert{7}
S:insert{1}
S:insert{0}
S:insert{0.1234}
S:insert{0.9884}
S:insert{1.234}

A = S:select{}

err = {}

for i = 1, #A - 1 do\
    if A[i][1] >= A[i+1][1] then\
        table.insert(err, {A[i][1], A[i+1][1]})\
    end\
end

err

S:drop()