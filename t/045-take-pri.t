#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(11)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')



local task1 = mq:put('tube1', { ttl = 1, pri = 10 }, 123)
test:ok(task1, 'task was put')

local task2 = mq:put('tube1', { ttl = 1, pri = 10 }, 124)
test:ok(task2, 'task was put again')

local pritask = mq:put('tube1', { ttl = 1, pri = 102 }, 125)
test:ok(pritask, 'priority task was put')

local taken = mq:take('tube1', 0.1)
test:ok(taken, 'task was taken')
test:is(taken[1], pritask[1], 'priority task id')


local taken2 = mq:take('tube1', 0.1)
test:ok(taken2, 'task was taken')
test:is(taken2[1], task1[1], 'one priorities are taked in put order')

tnt.finish()
os.exit(test:check() == true and 0 or -1)



