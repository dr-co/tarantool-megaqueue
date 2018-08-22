#!/usr/bin/env tarantool

local json = require 'json'
local test = require('tap').test()
local fiber = require 'fiber'
local fio = require 'fio'
test:plan(20)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')

local listen = fio.pathjoin(tnt.dir(), 'socket.tnt')

tnt.cfg{ listen = listen }

box.schema.user.create('test', { password = 'test' })
box.schema.user.grant(
    'test',
    'read,write,execute',
    'universe',
    nil,
    { if_not_exists = true }
);


listen = 'test:test@' .. listen 

_G.mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')


local nbox = require('net.box').new(listen)
test:ok(nbox:ping() == true, 'ping')

local nbox2 = require('net.box').new(listen)
test:ok(nbox2:ping() == true, 'ping 2th connector')

local task = mq:put('tube', nil, 123)
test:ok(task, 'task was put')

local task2 = mq:put('tube', nil, 345)
test:ok(task2, 'task was put')

local taken = nbox:call('mq:take', { 'tube', 0.5 })
test:ok(taken, 'task was taken through net.box')
test:isnt(taken[7], box.session.id(), 'its own session.id')


local taken2 = nbox2:call('mq:take', { 'tube', 0.5 })
test:ok(taken2, 'task was taken through net.box')
test:isnt(taken2[7], taken[7], 'session id')
test:isnt(taken2[7], box.session.id(), 'its own session.id')


---------


nbox:close()
test:ok(nbox:ping() == false, 'disconnected')

local retaken = mq:take('tube', 0.5)
test:ok(retaken, 'retake the task again (ready after reconnect)')
test:is(retaken[1], task[1], 'id')
test:is(retaken[7], box.session.id(), 'session id')

local retaken2 = mq:take('tube', 0.1)
test:ok(retaken2 == nil, 'do not retake the other sessions tasks')

local t2 = box.space.MegaQueue:get(task2[1])
test:ok(t2, 'select task2')
test:is(t2[5], 'work', 'its status unchanged')


--test:diag(tnt.log())
tnt.finish()
os.exit(test:check() == true and 0 or -1)




