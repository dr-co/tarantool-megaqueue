#!/usr/bin/env tarantool

local json = require 'json'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(12)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')

local task = mq:put('tube1', { ttl = 1 }, 123)
test:ok(task ~= nil, 'task was put')
local taken = mq:take('tube1', 0.01)
test:ok(taken ~= nil, 'task was taken')

local buried = mq:bury(taken[1], 'comment')
test:ok(buried, 'bury done')
test:is(buried[5], 'buried', 'status')
test:is(buried[6], buried[8].ttl + buried[8].created, 'event')
test:is(buried[8].bury_comment, 'comment', 'comment')


local res = mq:kick('tube1', 222)
test:is(#res, 1, 'one task was kicked')
test:is(res[1][1], task[1], 'task id')

-- print(tnt.log())
-- print(yaml.encode(box.space._space.index.name:select('MegaQueue')))

tnt.finish()
os.exit(test:check() == true and 0 or -1)




