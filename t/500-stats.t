#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(9)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')


test:test("put ready task", function(test)

    test:plan(8)

    local task = mq:put('tube1', { ttl = 1 }, 123)
    test:ok(task, 'task was put')

    test:is(task[2], 'tube1', 'tube name')
    test:is(task[4], '', 'task domain')
    test:is(task[5], 'ready', 'task status')
    test:ok(task[6] <= fiber.time() + mq.defaults.ttl, 'next event at ttl')
    test:is(task[7], 0, 'task client id')
    test:ok(task[8].created <= fiber.time(), 'task created')
    test:is(task[9], 123, 'task data')
end)

test:test("take ready task", function(test)

    test:plan(8)

    local task = mq:take('tube1', .1)
    test:ok(task, 'task was taken')

    test:is(task[2], 'tube1', 'tube name')
    test:is(task[4], '', 'task domain')
    test:is(task[5], 'work', 'task status')
    test:ok(task[6] <= fiber.time() + mq.defaults.ttl, 'next event at ttl')
    test:is(task[7], box.session.id(), 'task client id')
    test:ok(task[8].created <= fiber.time(), 'task created')
    test:is(task[9], 123, 'task data')
end)

test:test("take timeout", function(test)
    test:plan(1)

    local task = mq:take('tube1', .1)
    test:ok(task == nil, 'timeout reached')
end)

test:is_deeply({ mq:stats()[1]:unpack() }, { 'tube1', { ready = 0, work = 1 } }, 'stats')
test:isnil(mq:stats('unknown tube'), 'stats by unknown tube')


tnt.finish()
os.exit(test:check() == true and 0 or -1)




