#!/usr/bin/env tarantool

local test = require('tap').test()
test:plan(9)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local fiber = require 'fiber'

box.schema.space.create('persistent', { if_not_exists = true, engine = 'memtx' })
box.schema.space.create('tmp',
    { if_not_exists = true, engine = 'memtx', temporary = true })

test:ok(box.space.persistent ~= nil, 'persistent created')
test:ok(box.space.tmp ~= nil, 'tmp created')

box.space.persistent:create_index('pk',
    { unique = true, parts = { 1, 'str' }, if_not_exists = true })

box.space.tmp:create_index('pk',
    { unique = true, parts = { 1, 'str' }, if_not_exists = true })


box.space.persistent:truncate()
box.space.tmp:truncate()

test:test('rollback test', function(test)
    
    test:plan(3)

    box.begin()

    local t = box.space.persistent:insert{ 'rollback', 'hello', 'world' }
    test:ok(t ~= nil, 'inserted in transaction')
    t = box.space.persistent:get{ 'rollback' }
    test:ok(t ~= nil, 'selected in transaction')

    box.rollback()

    t = box.space.persistent:get{ 'rollback' }
    test:ok(t == nil, 'Did not written to disk')
end)

test:test('rollback by yieild test', function(test)
    
    test:plan(5)

    box.begin()

    local t = box.space.persistent:insert{ 'rollback', 'hello', 'world' }
    test:ok(t ~= nil, 'inserted in transaction')
    t = box.space.persistent:get{ 'rollback' }
    test:ok(t ~= nil, 'selected in transaction')

    fiber.sleep(0.01)
    local s, m = pcall(function() box.commit() end)

    test:ok(s == false, 'exception by box.commit')
    test:like(m, 'Transaction has been aborted', 'exception message')
    t = box.space.persistent:get{ 'rollback' }
    test:ok(t == nil, 'Did not written to disk')
end)

test:test('rollback [temporary space]', function(test)
    
    test:plan(3)

    box.begin()

    local t = box.space.tmp:insert{ 'rollback', 'hello', 'world' }
    test:ok(t ~= nil, 'inserted in transaction')
    t = box.space.tmp:get{ 'rollback' }
    test:ok(t ~= nil, 'selected in transaction')

    box.rollback()

    t = box.space.tmp:get{ 'rollback' }
    test:ok(t == nil, 'Did not written to disk')
end)

test:test('rollback by yieild test [temporary space]', function(test)
    
    test:plan(5)

    box.begin()

    local t = box.space.tmp:insert{ 'rollback', 'hello', 'world' }
    test:ok(t ~= nil, 'inserted in transaction')
    t = box.space.tmp:get{ 'rollback' }
    test:ok(t ~= nil, 'selected in transaction')

    fiber.sleep(0.01)
    local s, m = pcall(function() box.commit() end )
    test:ok(s == false, 'exception')
    test:like(m, 'Transaction has been aborted', 'exception message')

    t = box.space.tmp:get{ 'rollback' }
    test:ok(t == nil, 'Did not written to disk')
end)

test:test('yieild on persistent', function(test)
    
    test:plan(2)

    local was_yieild = false
    fiber.create(function() fiber.yield() was_yieild = true end)
    box.begin()
    box.space.persistent:replace{ 'rollback', 'hello', 'world' }
    test:is(was_yieild, false, 'do not yieild in transaction')
    box.commit()

    fiber.sleep(0)
    test:ok(was_yieild, 'box.commit touched yieild')
end)

test:test('yieild on temp', function(test)
    test:plan(4)
    local was_yieild = false
    fiber.create(function() finish.yieild() was_yieild = true end)

    box.space.tmp:insert{ 'rollback1', 'hello', 'world' }
    test:is(was_yieild, false, 'tempspace:insert does not touch yieild')
    
    box.space.tmp:replace{ 'rollback', 'hello', 'world' }
    test:is(was_yieild, false, 'tempspace:replace does not touch yieild')
    
    box.space.tmp:update( 'rollback', {{ '=', 2, 'ehlo' }} )
    test:is(was_yieild, false, 'tempspace:update does not touch yieild')
    
    box.space.tmp:delete{ 'rollback1' }
    test:is(was_yieild, false, 'tempspace:delete does not touch yieild')
end)


tnt.finish()
os.exit(test:check() == true and 0 or -1)

