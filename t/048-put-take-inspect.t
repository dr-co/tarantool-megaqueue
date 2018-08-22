#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(27)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')

test:test('put inspector first', function(test) 
    test:plan(8)


    local ti = mq:put('tube1', { ttl = 1, domain = 'abc', inspect = 1 }, 'inspect')
    test:ok(ti, 'task 2 was put')
    test:is(ti[4], 'abc', 'domain')
    test:isnil(ti[8].domain, 'opts.domain')
    test:is(ti[8].inspect, 1, 'opts.inspect')
    test:is(ti[5], 'ready', 'status')

    local tit = mq:take('tube1', .1)
    test:is(tit[1], ti[1], 'task inspect was taken')
    test:is(tit[5], 'work', 'status')
    test:ok(mq:ack(tit), 'ack')
end)

test:test('put inspector first and twice', function(test) 
    test:plan(16)
    
    local ti = mq:put('tube1', { ttl = 1, domain = 'abc', inspect = 1 }, 'inspect1')
    test:ok(ti, 'task was put')
    test:is(ti[4], 'abc', 'domain')
    test:isnil(ti[8].domain, 'opts.domain')
    test:is(ti[8].inspect, 1, 'opts.inspect')
    test:is(ti[5], 'ready', 'status')

    local ti2 = mq:put('tube1', { ttl = 1, domain = 'abc', inspect = 1 }, 'inspect2')
    test:ok(ti2, 'task 2 was put')
    test:is(ti2[4], 'abc', 'domain')
    test:isnil(ti2[8].domain, 'opts.domain')
    test:is(ti2[8].inspect, 1, 'opts.inspect')
    test:is(ti2[5], 'inspect', 'status')
    
    local tit = mq:take('tube1', .1)
    test:is(tit[1], ti[1], 'task inspect was taken')
    test:is(tit[5], 'work', 'status')
    test:ok(mq:ack(tit), 'ack')
    
    local tit2 = mq:take('tube1', .1)
    test:is(tit2[1], ti2[1], 'task2 inspect was taken')
    test:is(tit2[5], 'work', 'status')
    test:ok(mq:ack(tit2), 'ack')
end)


local t1 = mq:put('tube1', { ttl = 1, domain = 'abc' }, 123)
test:ok(t1, 'task 1 was put')
test:is(t1[4], 'abc', 'domain')
test:isnil(t1[8].domain, 'opts.domain')
test:is(t1[5], 'ready', 'status')

local ti = mq:put('tube1', { ttl = 1, domain = 'abc', inspect = 1 }, 'inspect')
test:ok(ti, 'task 2 was put')
test:is(ti[4], 'abc', 'domain')
test:isnil(ti[8].domain, 'opts.domain')
test:is(ti[5], 'inspect', 'status')

local t2 = mq:put('tube1', { ttl = 1, domain = 'abc' }, 345)
test:ok(t2, 'task 2 was put')
test:is(t2[4], 'abc', 'domain')
test:isnil(t2[8].domain, 'opts.domain')
test:is(t2[5], 'wait', 'status')


local t1t = mq:take('tube1', .1)
test:is(t1t[1], t1[1], 'task 1 was taken')
test:is(t1t[5], 'work', 'status')
test:ok(mq:ack(t1t), 'ack')

local t2t = mq:take('tube1', .1)
test:is(t2t[1], t2[1], 'task 2 was taken')
test:is(t2t[5], 'work', 'status')
test:ok(mq:ack(t2t), 'ack')

local tit = mq:take('tube1', .1)
test:is(tit[1], ti[1], 'task inspect was taken')
test:is(tit[5], 'work', 'status')
test:ok(mq:ack(tit), 'ack')

tnt.finish()
os.exit(test:check() == true and 0 or -1)



