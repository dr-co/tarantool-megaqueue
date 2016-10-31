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



local task1 = mq:put('tube2', { ttl = 1, pri = 10 }, 123)
test:ok(task1, 'task was put')

local task2 = mq:put('tube4', { ttl = 1, pri = 10 }, 124)
test:ok(task2, 'task was put again')


test:isnil(mq:take('tube1', 0.1), 'take tube1 timeout')
test:isnil(mq:take('tube3', 0.1), 'take tube3 timeout')
test:isnil(mq:take('tube5', 0.1), 'take tube5 timeout')

tnt.finish()
os.exit(test:check() == true and 0 or -1)




