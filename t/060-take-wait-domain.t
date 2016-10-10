#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(6)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')

local started = fiber.time()
fiber.create(function()
    fiber.sleep(0.25)
    mq:put('tube1', { ttl = 5, domain = 'abc' }, 123)
    mq:put('tube1', { ttl = 5, domain = 'abc' }, 345)

end)


test:test("take ready task", function(test)

    test:plan(11)


    local task = mq:take('tube1', 0.5)
    test:ok(task, 'task was taken')

    test:is(task[2], 'tube1', 'tube name')
    test:is(task[4], 'abc', 'task domain')
    test:is(task[5], 'work', 'task status')
    test:ok(task[6] <= fiber.time() + mq.defaults.ttl, 'next event at ttl')
    test:is(task[7], box.session.id(), 'task client id')
    test:ok(task[8].created <= fiber.time(), 'task created')
    test:is(task[9], 123, 'task data')
    test:ok(fiber.time() - started >= 0.25, 'waiting time')
    test:ok(fiber.time() - started < 0.35, 'waiting time')

    task = mq:take('tube1', 0.1)
    test:ok(task == nil, 'second task is not taken')
end)




----------------------------------------------------
test:test('domain ttl', function(test)
    test:plan(5)
    local task = mq:put('tube2', { ttl = 0.1, domain = 'abc' })
    test:is(task[5], 'ready', 'task1 was put (ttl = 0.1)')

    local ltask = mq:put('tube2', { domain = 'abc' })
    test:is(ltask[5], 'wait', 'task2 was put and wait')

    fiber.sleep(0.3)

    local ttl = box.space.MegaQueue.index.id:get(task[1])
    test:ok(ttl == nil, 'task was removed by ttl')

    local taken = mq:take('tube2', 0.5)

    test:ok(taken ~= nil, 'task2 was taken')
    test:is(taken[1], ltask[1], 'task2 id')

    
end)



-- test:diag(tnt.log())
----------------------------------------------------
tnt.finish()
os.exit(test:check() == true and 0 or -1)



