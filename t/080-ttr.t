#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(8)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')

test:test('put task', function(test)
    test:plan(2)
    local task = mq:put('tube1', { ttr = .25 }, 123)
    test:ok(task ~= nil, 'task was put')
    test:is(task[5], 'ready', 'state')
end)

test:test("take ready task", function(test)

    test:plan(15)


    local started = fiber.time()
    local task = mq:take('tube1', 1)
    test:ok(task, 'task was taken')

    test:is(task[2], 'tube1', 'tube name')
    test:is(task[4], '', 'task domain')
    test:is(task[5], 'work', 'task status')
    test:ok(task[6] <= fiber.time() + mq.defaults.ttl, 'next event at ttl')
    test:is(task[7], box.session.id(), 'task client id')
    test:ok(task[8].created <= fiber.time(), 'task created')
    test:is(task[9], 123, 'task data')
    test:ok(fiber.time() - started <= 0.05, 'waiting time')

    local t = mq:take('tube1', 2)
    if not test:ok(t ~= nil, 'task was retaken') then
        return
    end
    
    test:is(t[1], task[1], 'task id')
    test:is(t[5], 'work', 'task status')
    test:is(t[7], box.session.id(), 'task client id')
    test:ok(fiber.time() >= started + 0.25, 'next event at ttr')
    test:ok(fiber.time() < started + 0.35, 'next event at ttl')
end)


test:test('prolong ttr tests', function(test)
    test:plan(45)

    test:ok(mq:put('tube_ttl', { ttr = .25 }, 123), 'put task with short ttl')

    local task = mq:take('tube_ttl', .1)
    test:ok(task, 'task was taken')
    test:is(task[5], 'work', 'task status')

    local started = fiber.time()
    for i = 1, 10 do
        fiber.sleep(0.1)
        local ptask = mq:prolong_ttr(task[1])
        test:ok(ptask, 'prolong done ' .. i)
        test:is(ptask[6], task[6] + 0.25, 'real ttr value')
        test:is(ptask[8].ttl, task[8].ttl + 0.25, 'ttl value')
        test:is(ptask[8].ttr, task[8].ttr, 'ttr is not changed')
        task = ptask
    end
    test:ok(fiber.time() - started > task[8].ttr * 2, 'timeout exceeded')

    test:ok(mq:take('tube_ttl', .5) == nil, 'task is taken yet')
end)

test:test('ttr after prolong', function(test)
    test:plan(10)

    test:ok(mq:put('tube_ttl', { ttr = .25 }, 123), 'put task with short ttl')

    local task = mq:take('tube_ttl', .1)
    test:ok(task, 'task was taken')
    test:is(task[5], 'work', 'task status')

    local started = fiber.time()
    
    local ptask = mq:prolong_ttr(task[1], .2)
    test:ok(ptask, 'prolong done')
    test:is(ptask[6], task[6] + 0.2, 'real ttr value')
    test:is(ptask[8].ttl, task[8].ttl + 0.2, 'ttl value')
    test:is(ptask[8].ttr, task[8].ttr, 'ttr is not changed')

    test:ok(mq:take('tube_ttl', .2) == nil, 'task is taken')
    test:ok(mq:take('tube_ttl', .2) == nil, 'task is taken yet')
    test:ok(mq:take('tube_ttl', .25), 'task was released')

end)

-- test:diag(tnt.log())
----------------------------------------------------
tnt.finish()
os.exit(test:check() == true and 0 or -1)



