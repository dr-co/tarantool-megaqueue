#!/usr/bin/env tarantool

local json = require 'json'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(7)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')



test:test('Ack by task id', function(test)
    
    test:plan(7)

    local task = mq:put('tube1', { ttl = 1 }, 123)
    test:ok(task ~= nil, 'task was put')

    local taken = mq:take('tube1', 0.01)
    test:ok(taken ~= nil, 'task was taken')

    local ack = mq:ack(taken[1])
    test:ok(ack ~= nil, 'task was acked')

    test:is(ack[5], 'removed', 'task status is removed')

    local db = box.space.MegaQueue:get(task[1])
    test:ok(db == nil, 'DB do not contain the task')


    local status, err = pcall(function() mq:ack(taken[1]) end)
    test:ok(not status, 'Ack removed task raise error')

    test:like(tostring(err), 'not found', 'Error message')

end)

test:test('Ack by tuple', function(test)
    
    test:plan(5)

    local task = mq:put('tube1', { ttl = 1 }, 123)
    test:ok(task ~= nil, 'task was put')

    local taken = mq:take('tube1', 0.01)
    test:ok(taken ~= nil, 'task was taken')

    local ack = mq:ack(taken)
    test:ok(ack ~= nil, 'task was acked')

    test:is(ack[5], 'removed', 'task status is removed')

    local db = box.space.MegaQueue:get(task[1])
    test:ok(db == nil, 'DB do not contain the task')

end)

test:test('Ack by table', function(test)
    
    test:plan(5)

    local task = mq:put('tube1', { ttl = 1 }, 123)
    test:ok(task ~= nil, 'task was put')

    local taken = mq:take('tube1', 0.01)
    test:ok(taken ~= nil, 'task was taken')

    local ack = mq:ack({ taken:unpack() })
    test:ok(ack ~= nil, 'task was acked')

    test:is(ack[5], 'removed', 'task status is removed')

    local db = box.space.MegaQueue:get(task[1])
    test:ok(db == nil, 'DB do not contain the task')

end)




-- print(tnt.log())
-- print(yaml.encode(box.space._space.index.name:select('MegaQueue')))

tnt.finish()
os.exit(test:check() == true and 0 or -1)



