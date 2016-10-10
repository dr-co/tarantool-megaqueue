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



test:test("put ready task", function(test)

    test:plan(9)
    local task = mq:put('tube1', { ttl = 1 }, 123)
    test:ok(task, 'task was put')

    test:is(task[2], 'tube1', 'tube name')
    test:is(task[3], 1000 - mq.defaults.pri, 'task pri')
    test:is(task[4], '', 'task domain')
    test:is(task[5], 'ready', 'task status')
    test:ok(task[6] <= fiber.time() + mq.defaults.ttl, 'next event at ttl')
    test:is(task[7], 0, 'task client id')
    test:ok(task[8].created <= fiber.time(), 'task created')
    test:is(task[9], 123, 'task data')
end)

test:test("put ready task with domain", function(test)
    
    test:plan(9)

    local task = mq:put('tube1', { ttl = 1, domain = 'abc' }, 345)
    test:ok(task, 'task was put')

    test:is(task[2], 'tube1', 'tube name')
    test:is(task[3], 1000 - mq.defaults.pri, 'task pri')
    test:is(task[4], 'abc', 'task domain')
    test:is(task[5], 'ready', 'task status')
    test:ok(task[6] <= fiber.time() + mq.defaults.ttl, 'next event at ttl')
    test:is(task[7], 0, 'task client id')
    test:ok(task[8].created <= fiber.time(), 'task created')
    test:is(task[9], 345, 'task data')

--    test:diag(yaml.encode(task))
end)


test:test("put second task with the same domain", function(test)
    test:plan(9)

    local task = mq:put('tube1', { ttl = 1, domain = 'abc' }, 345)
    test:ok(task, 'task was put')

    test:is(task[2], 'tube1', 'tube name')
    test:is(task[3], 1000 - mq.defaults.pri, 'task pri')
    test:is(task[4], 'abc', 'task domain')
    test:is(task[5], 'wait', 'task status')
    test:ok(task[6] <= fiber.time() + mq.defaults.ttl, 'next event at ttl')
    test:is(task[7], 0, 'task client id')
    test:ok(task[8].created <= fiber.time(), 'task created')
    test:is(task[9], 345, 'task data')
end)

test:test("put ready task", function(test)

    test:plan(9)

    local task = mq:put('tube1', { ttl = 1 }, 123)
    test:ok(task, 'task was put')

    test:is(task[2], 'tube1', 'tube name')
    test:is(task[3], 1000 - mq.defaults.pri, 'task pri')
    test:is(task[4], '', 'task domain')
    test:is(task[5], 'ready', 'task status')
    test:ok(task[6] <= fiber.time() + mq.defaults.ttl, 'next event at ttl')
    test:is(task[7], 0, 'task client id')
    test:ok(task[8].created <= fiber.time(), 'task created')
    test:is(task[9], 123, 'task data')
end)



-- print(tnt.log())
-- print(yaml.encode(box.space._space.index.name:select('MegaQueue')))

tnt.finish()
os.exit(test:check() == true and 0 or -1)


