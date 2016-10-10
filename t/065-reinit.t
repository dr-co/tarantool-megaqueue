#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(17)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')

local started = fiber.time()


local task1 = mq:put('tube1', { ttl = 5, domain = 'abc' }, 123)
test:ok(task1, 'task1 was put')
test:is(task1[5], 'ready', 'status')

local task2 = mq:put('tube1', { ttl = 5, domain = 'abc' }, 345)
test:ok(task2, 'task2 was put')
test:is(task2[5], 'wait', 'status')

local taken1 = mq:take('tube1', 0.5)
test:ok(taken1, 'task1 was taken')
test:is(taken1[1], task1[1], 'id')

local taken2 = mq:take('tube1', 0.1)
test:ok(taken2 == nil, 'task2 is waiting')



test:is(mq:init(), 0, 'Next init do not upgdate database')

local status, err = pcall(function() mq:ack(taken1[1]) end)
test:ok(not status, 'Do not ack after reinit')
test:like(err, 'was not taken', 'error text')


local retaken1 = mq:take('tube1', 0.5)
test:ok(retaken1, 'task1 was retaken')
test:is(retaken1[1], taken1[1], 'id')


local acked = mq:ack(taken1[1])
test:ok(acked, 'task was acked')

-- test:diag(tnt.log())
----------------------------------------------------
tnt.finish()
os.exit(test:check() == true and 0 or -1)



