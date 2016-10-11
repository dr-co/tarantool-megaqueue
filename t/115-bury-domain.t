#!/usr/bin/env tarantool

local json = require 'json'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(20)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')

local task = mq:put('tube1', { domain = 'test' }, 123)
test:ok(task, 'task was put')
test:is(task[5], 'ready', 'status')

local second = mq:put('tube1', { domain = 'test' }, 345)
test:ok(second, 'second task was put')
test:is(second[5], 'wait', 'status')

local taken = mq:take('tube1')
test:ok(taken, 'task was taken')
test:is(taken[1], task[1], 'id')
local buried = mq:bury(taken)
test:ok(buried, 'task was buried')
test:is(buried[5], 'buried', 'status')

local second_taken = mq:take('tube1', 0.1)
test:ok(second_taken, 'second task was taken')

local unbury = mq:dig(buried)
test:ok(unbury, 'task was unburied')
test:is(unbury[5], 'wait', 'status')

local removed = mq:delete(unbury)
test:ok(removed, 'task was removed')
test:is(removed[5], 'removed', 'task status')
test:isnil(box.space.MegaQueue:get(removed[1]), 'removed in database')

local peeked = mq:peek(second_taken)
test:ok(peeked, 'task was peeked')
test:is_deeply(peeked, second_taken, 'task tuple')

-- print(tnt.log())
-- print(yaml.encode(box.space._space.index.name:select('MegaQueue')))

tnt.finish()
os.exit(test:check() == true and 0 or -1)




