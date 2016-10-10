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

test:test('put tasks', function(test)
    test:plan(4)
    local task = mq:put('tube1', { ttl = .25, domain = 'abc' }, 123)
    test:ok(task ~= nil, 'task was put')
    test:is(task[5], 'ready', 'state')
    
    local task2 = mq:put('tube1', { domain = 'abc' }, 345)
    test:ok(task2, 'task was put')
    test:is(task2[5], 'wait', 'state')
end)

test:test("take ready task", function(test)

    test:plan(10)


    local started = fiber.time()

    local task = mq:take('tube1', 1)
    test:ok(task, 'task was taken')
    test:is(task[5], 'work', 'task status')
    test:is(task[9], 123, 'task data')
    test:ok(fiber.time() - started <= 0.05, 'waiting time')

    
    
    local t = mq:take('tube1', 2)
    if not test:ok(t, 'task2 was taken') then
        return
    end
    
    test:is(t[9], 345, 'second task really')
    test:is(t[5], 'work', 'task status')
    test:is(t[7], box.session.id(), 'task client id')
    test:ok(fiber.time() >= started + 0.25, 'after ttl')
    test:ok(fiber.time() < started + 0.35, 'before timeout')
end)



-- test:diag(tnt.log())
----------------------------------------------------
tnt.finish()
os.exit(test:check() == true and 0 or -1)



