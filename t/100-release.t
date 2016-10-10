#!/usr/bin/env tarantool

local json = require 'json'
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

test:test('Release to delayed', function(test)
    
    test:plan(10)

    local task = mq:put('tube1', { ttl = 1 }, 123)
    test:ok(task ~= nil, 'task was put')

    local taken = mq:take('tube1', 0.01)
    test:ok(taken ~= nil, 'task was taken')

    local released = mq:release(taken[1], 2)
    test:ok(released ~= nil, 'task was acked')

    test:is(released[5], 'delayed', 'task status is delayed')
    test:ok(released[8].ttl - taken[8].ttl >= 2, 'TTL is prolonged')
    test:ok(released[8].ttl - taken[8].ttl < 2.1, 'TTL is prolonged to delay')

    test:ok(released[6] - fiber.time() >= 1.9, 'next event')
    test:ok(released[6] - fiber.time() <= 2.1, 'next event')
    
    local status, err = pcall(function() mq:release(taken[1]) end)
    test:ok(not status, 'Ack removed task raise error')

    test:like(tostring(err), 'was not taken', 'Error message')
end)

test:test('Release to ready', function(test)
    
    test:plan(12)

    local task = mq:put('tube1', { ttl = 1, domain = 'abc' }, 123)
    test:ok(task ~= nil, 'task was put')
    
    local task_wait = mq:put('tube1', { ttl = 1, domain = 'abc' }, 345)
    test:ok(task_wait ~= nil, 'second task')
    test:is(task_wait[5], 'wait', 'waiting')

    local taken = mq:take('tube1', 0.01)
    test:ok(taken ~= nil, 'task was taken')

    local released = mq:release(taken[1])
    test:ok(released ~= nil, 'task was released')

    test:is(released[5], 'ready', 'task status is ready')
    
    local retaken = mq:take('tube1', 0.01)
    test:ok(retaken ~= nil, 'task was retaken')
    test:is(retaken[1], taken[1], 'the same tasks')

    local ack = mq:ack(retaken[1])
    test:ok(ack ~= nil, 'ack')
    test:is(ack[5], 'removed', 'task was removed')
    
    
    local take2 = mq:take('tube1', 0.01)
    test:ok(take2 ~= nil, 'take wait task')
    test:is(take2[1], task_wait[1], 'wait task id')
end)




-- print(tnt.log())
-- print(yaml.encode(box.space._space.index.name:select('MegaQueue')))

tnt.finish()
os.exit(test:check() == true and 0 or -1)



