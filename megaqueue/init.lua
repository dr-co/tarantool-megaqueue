local log                   = require 'log'
local fiber                 = require 'fiber'

local MAX_PRI               = 1000

local ID                    = 1
local TUBE                  = 2
local PRI                   = 3
local DOMAIN                = 4
local STATUS                = 5
local EVENT                 = 6
local CLIENT                = 7
local OPTIONS               = 8
local DATA                  = 9
                           
                           
local C_ID                  = 1
local C_TUBE                = 2
local C_CLIENT              = 3
local C_FID                 = 4

local TIMEOUT_INFINITY      = 86400 * 365 * 100

local mq = {
    VERSION             = '1.0',

    defaults    = {
        ttl             = 86400,
        ttr             = 86400,
        pri             = 50,
        domain          = '',
        delay           = 0,
    },

    -- last serials
    private = {
        serial  = {},

        migrations  = require('megaqueue.migrations'),
        consumer    = {},
        
        may_enqueue = {
            ready   = true,
            work    = true,
            buried  = true
        },

        stats       = require('megaqueue.stats')
    }
}

-- consumers control API
function mq._consumer_wakeup(self, tube)
    if self.private.consumer[tube] == nil then
        return
    end

    for fid, bool in pairs(self.private.consumer[tube]) do
        if bool then
            self.private.consumer[tube][fid] = nil
            fiber.find(fid):wakeup()
            break
        end
    end
end

function mq._consumer_sleep(self, tube, timeout)
    
    local fid = fiber.id()
    
    if self.private.consumer[tube] == nil then
        self.private.consumer[tube] = {}
    end

    self.private.consumer[tube][fid] = true
    
    fiber.sleep(timeout)
    
    self.private.consumer[tube][fid] = nil
end

function mq._consumer_reinit(self)
    local list = self.private.consumer
    self.private.consumer = {}
    for _, tube in pairs(list) do
        for fid, bool in pairs(tube) do
            if bool then
                fiber.find(fid):wakeup()
            end
        end
    end
end

-- Miscellaneous
function mq._extend(self, t1, t2)
    local res = {}

    if t1 ~= nil then
        for k, v in pairs(t1) do
            res[k] = v
        end
    end

    if t2 ~= nil then
        for k, v in pairs(t2) do
            if res[k] ~= nil and v ~= nil and type(res[k]) ~= type(v) then
                box.error(box.error.PROC_LUA,
                    string.format(
                        'Wrong type for ".%s": %s (have to be %s)',
                        tostring(k),
                        type(v),
                        type(res[k])
                    )
                )
            end
            res[k] = v
        end
    end

    return res
end

-- Autoincrement
function mq._serial(self, space)
    if self.private.serial[space] == nil then
        local max = box.space[space].index.id:max()
        if max ~= nil then
            self.private.serial[space] = max[1]
        else
            self.private.serial[space] = tonumber64(0)
        end
    end
    return self.private.serial[space] + tonumber64(1)
end

function mq._next_serial(self, space)
    self.private.serial[space] = self:_serial(space)
end


-- try take task
function mq._take(self, tube)
    local task = box.space.MegaQueue.index.tube_status_pri_id
                                :min{ tube, 'ready' }

    if task == nil or task[STATUS] ~= 'ready' then
        return self:_normalize_task()
    end

    local ttl = task[OPTIONS].created + task[OPTIONS].ttl
    local ttr = task[OPTIONS].ttr + fiber.time()

    if ttr > ttl then
        ttr = ttl
    end

    box.begin()

        local old_status = task[STATUS]
        task = box.space.MegaQueue:update(
                task[ID], {
                    { '=', STATUS, 'work' },
                    { '=', CLIENT, box.session.id() },
                    { '=', EVENT, ttr }
                })
        self.private.stats:inc(task[TUBE], 'work', old_status)
    box.commit()

    return task
end

function mq._normalize_task(self, task)
    if task == nil then
        return
    end
    return task
end

function mq._task_by_tube_domain(self, tube, domain, statuses)
    local list

    for _, status in pairs(statuses) do
        list = box.space.MegaQueue.index.tube_domain_status
                    :select({ tube, domain, status },
                                { iterator = 'EQ', limit = 1 })
        if #list > 0 then
            return list[1]
        end
    end
end

function mq._run_worker(self)
    local rw = self._run_fiber

    fiber.create(function()
        local now
        while rw[1] do
            now = fiber.time()
            local task = box.space.MegaQueue.index.event:min()
            if task == nil then
                rw[2] = fiber.id()
                fiber.sleep(3600)
                rw[2] = nil
            else

                if task[EVENT] > now then
                    rw[2] = fiber.id()
                    fiber.sleep(task[EVENT] - now)
                    rw[2] = nil

                else
                    -- ttl works in ANY status
                    if task[OPTIONS].ttl + task[OPTIONS].created <= now then
                        self:_task_delete(task, 'TTL')


                    -- ttr
                    elseif task[STATUS] == 'work' then
                        self:_task_to_ready(task)

                    -- delayed to ready
                    elseif task[STATUS] == 'delayed' then
                        self:_task_to_ready(task)
                    else
                        error(
                            string.format(
                                'Internal error: event on task [%s]',
                                    require('json').encode(task)
                            )
                        )
                    end
                end
            end
        end
    end)
end

function mq._enqueue_task_by(self, task)

    

    if not self.private.may_enqueue[ task[STATUS] ] then
        return
    end

    if task[DOMAIN] == '' then
        return
    end

    -- check if error (impossible, but...)
    local exists =
        self:_task_by_tube_domain(
            task[TUBE],
            task[DOMAIN],
            { 'ready', 'work' }
        )

    if exists ~= nil then
        return
    end

    local wait_task =
        self:_task_by_tube_domain(
            task[TUBE],
            task[DOMAIN],
            { 'wait' }
        )

    if wait_task == nil then
        return
    end

    box.begin()
        local old_status = wait_task[STATUS]
        box.space.MegaQueue:update(wait_task[ID],
            {
                { '=', STATUS, 'ready' },
                { '=', CLIENT, 0 },
                { '=', EVENT,
                        wait_task[OPTIONS].ttl + wait_task[OPTIONS].created }
            }
        )
        self.private.stats:inc(task[TUBE], 'ready', old_status)
    box.commit()
    self:_consumer_wakeup(wait_task[TUBE])
end

function mq._task_delete(self, task, reason)
    local rm_task
    box.begin()
        local old_status = task[STATUS]
        rm_task = box.space.MegaQueue:delete(task[ID])
        self.private.stats:dec(task[TUBE], old_status)
    box.commit()
        
    self:_enqueue_task_by(task)

    if rm_task ~= nil then
        return rm_task:transform(STATUS, 1, 'removed')
    end
end

function mq._task_to_ready(self, task, prolong_ttl)

    local status = 'ready'
    local event = task[OPTIONS].created + task[OPTIONS].ttl

    if task[DOMAIN] ~= '' then
        local ck_statuses
        if task[STATUS] == 'work' then
            ck_statuses = { 'ready' }
        else
            ck_statuses = { 'ready', 'work' }
        end
        
        local exists =
            self:_task_by_tube_domain(
                task[TUBE],
                task[DOMAIN],
                ck_statuses
            )
        if exists ~= nil then
            status = 'wait'
        end
    end

    local opts = task[OPTIONS]
    if prolong_ttl then
        opts.ttl = opts.ttl + fiber.time() - opts.created()
    end

    local updated
    box.begin()
        updated = box.space.MegaQueue:update(task[ID], {
            { '=', STATUS, status },
            { '=', EVENT, event },
            { '=', CLIENT, 0 },
            { '=', OPTIONS, opts }
        })
        
        self.private.stats:inc(task[TUBE], status, task[STATUS])
        
        self:_consumer_wakeup(task[TUBE])
        -- TODO: statistics
    box.commit()
    return updated
end

function mq._process_tube(self, tube)
    if self._run_fiber == nil then
        return
    end
    if self._run_fiber[2] == nil then
        return
    end
    local fid = self._run_fiber[2]
    self._run_fiber[2] = nil
    fiber.find(fid):wakeup()
end

function mq._tid_by_task_or_tid(self, tid, usage)
    if tid == nil then
        box.error(box.error.PROC_LUA, usage)
    end

    if type(tid) == 'table' or type(tid) == 'cdata' then
        return tonumber64(tid[1])
    end
    return tonumber64(tid)
end

function mq._get_taken(self, tid)
    local task = box.space.MegaQueue:get(tid)
    if task == nil then
        box.error(box.error.PROC_LUA, string.format('Task %s not found', tid))
    end

    if task[CLIENT] ~= box.session.id() then
        box.error(box.error.PROC_LUA,
            string.format(
                'Task %s was not taken (or was released by TTR)', tid)
        )
    end

    if task[STATUS] ~= 'work' then
        box.error(box.error.PROC_LUA,
            string.format(
                'Task %s is not in work status (%s)',
                tid,
                task[STATUS]
            )
        )
    end
    return task
end

------------------------------------------------------------------------------
-- API
------------------------------------------------------------------------------

function mq.take(self, tube, timeout)
    if timeout == nil then
        timeout = TIMEOUT_INFINITY
    else
        timeout = tonumber(timeout)
    end

    tube = tostring(tube)

    local started = fiber.time()

    while timeout >= 0 do

        local task = self:_take(tube)
        if task ~= nil then
            return self:_normalize_task(task)
        end

        if timeout <= 0 then
            return
        end

        self:_consumer_sleep(tube, timeout)

        local now = fiber.time()
        timeout = timeout - (now - started)
        started = now

        if timeout < 0 then
            return self:_normalize_task(self:_take(tube))
        end
    end
end

function mq.put(self, tube, opts, data)
    opts = self:_extend(self.defaults, opts)

    -- perl or some the othe langs can't recognize 1 and '1'
    opts.domain = tostring(opts.domain)
    tube = tostring(tube)


    local status = 'ready'
    if opts.delay > 0 then
        opts.ttl = opts.ttl + opts.delay
        status = 'delayed'
    elseif opts.domain ~= '' then
        -- checks domain
        local exists =
            self:_task_by_tube_domain(tube, opts.domain, { 'ready', 'work' })

        if exists ~= nil then
            status = 'wait'
        end
    end

    local event

    opts.created = fiber.time()

    if status == 'delayed' then
        event = opts.created + opts.delay
    else
        event = opts.created + opts.ttl
    end

    if opts.pri > MAX_PRI then
        opts.pri = MAX_PRI
    elseif opts.pri < 0 then
        opts.pri = 0
    end
    local pri = MAX_PRI - opts.pri
    local domain = opts.domain
    opts.domain = nil

    local task = box.tuple.new {
        [ID]        = self:_serial('MegaQueue'),
        [TUBE]      = tube,
        [PRI]       = pri,
        [DOMAIN]    = domain,
        [STATUS]    = status,
        [EVENT]     = event,
        [CLIENT]    = 0,
        [OPTIONS]   = opts,
        [DATA]      = data,
    }

    local consumer
    box.begin()
        task = box.space.MegaQueue:insert(task)
        self:_next_serial('MegaQueue')

        
        self.private.stats:inc(task[TUBE], task[STATUS])

        self:_consumer_wakeup(tube)
    box.commit()

    self:_process_tube(task)

    return self:_normalize_task(task)
end

function mq.ack(self, tid)

    tid = self:_tid_by_task_or_tid(tid, 'usage: mq:ack(task_id)')
    local task = self:_get_taken(tid)
    return self:_normalize_task(self:_task_delete(task))
end

function mq.release(self, tid, delay)
    tid = self:_tid_by_task_or_tid(tid, 'usage: mq:release(task_id)')
    if delay ~= nil then
        delay = tonumber(delay)
        if delay < 0 then
            delay = 0
        end
    else
        delay = 0
    end
    local task = self:_get_taken(tid)

    if delay > 0 then
        local opts = task[OPTIONS]
        opts.ttl = opts.ttl + fiber.time() - opts.created + delay
        local event = fiber.time() + delay
       
        local old_status = task[STATUS]
        box.begin()
            task = box.space.MegaQueue:update(task[ID],
                {
                    { '=', STATUS, 'delayed' },
                    { '=', CLIENT, 0 },
                    { '=', EVENT, event },
                    { '=', OPTIONS, opts }
                }
            )
            self.private.stats:inc(task[TUBE], 'delayed', old_status)
        box.commit()
        self:_run_worker()
        return self:_normalize_task(task)
    end
    return self:_normalize_task(self:_task_to_ready(task))
end


function mq.bury(self, tid, comment)
    tid = self:_tid_by_task_or_tid(tid, 'usage: mq:bury(task_id)')
    local task = self:_get_taken(tid)
   
    local opts = task[OPTIONS]
    opts.bury_comment = comment
    local old_status = task[STATUS]
    box.begin()
        task = box.space.MegaQueue:update(task[ID],
            {
                { '=', STATUS, 'buried' },
                { '=', CLIENT, 0 },
                { '=', EVENT, task[OPTIONS].created + task[OPTIONS].ttl },
                { '=', OPTIONS, opts }
            }
        )
        self.private.stats:inc(task[TUBE], 'work', old_status)
    box.commit()
    self:_enqueue_task_by(task)
    return self:_normalize_task(task)
end

function mq.dig(self, tid)
    tid = self:_tid_by_task_or_tid(tid, 'usage: mq:dig(task_id)')
    local task = box.space.MegaQueue:get(tid)
    if task == nil then
        box.error(box.error.PROC_LUA,
            string.format(
                'Task %s is not found', tostring(tid)
            )
        )
    end

    if task[STATUS] ~= 'buried' then
        box.error(box.error.PROC_LUA,
            string.format(
                'Task %s is not buried (%s)', tostring(tid), task[STATUS]
            )
        )
    end
    return self:_normalize_task(self:_task_to_ready(task))
end

mq.unbury = mq.dig

function mq.delete(self, tid)
    tid = self:_tid_by_task_or_tid(tid, 'usage: mq:delete(task_id)')
    local task = box.space.MegaQueue:get(tid)
    return self:_normalize_task(self:_task_delete(task))
end

function mq.peek(self, tid)
    tid = self:_tid_by_task_or_tid(tid, 'usage: mq:peek(task_id)')
    local task = box.space.MegaQueue:get(tid)
    return self:_normalize_task(task)
end


function mq.kick(self, tube, count)
    if count == nil then
        count = 1
    end
    count = tonumber(count)
    if count < 1 then
        return
    end

    local list = box.space.MegaQueue.index.tube_status_pri_id
        :select({ tube, 'buried' }, { limit = count, iterator = 'EQ' })

    local res = {}
    for _, task in pairs(list) do
        table.insert(res, self:_task_to_ready(task))
    end
    if #res == 0 then
        return
    end
    return res
end

function mq.init(self, defaults)

    self.defaults = self:_extend(self.defaults, defaults)

    local upgrades = self.private.migrations:upgrade(self)
    log.info('MegaQueue started')

    if self._run_fiber ~= nil then
        self._run_fiber[1] = false
    end

    self._run_fiber = { true }

    while true do
        local task = box.space.MegaQueue.index.status_client:min('work')
        if task == nil then
            break
        end
        if task[STATUS] ~= 'work' then
            break
        end
        self:_task_to_ready(task)
    end

    local list = box.session.on_disconnect()
    for _, cb in pairs(list) do
        box.session.on_disconnect(nil, cb)
    end

    box.session.on_disconnect(self:_on_disconnect())

    self:_consumer_reinit()
    self:_run_worker()


    return upgrades
end


function mq.stats(self, tube)
    if tube == nil then
        return box.space.MegaQueueStats:select(nil, { iterator = 'ALL' })
    end
    return box.space.MegaQueueStats:get(tube)
end

mq.statistics = mq.stats

function mq._on_disconnect(self)
    return function()

        local client = box.session.id()
        local rf = self._run_fiber

        log.info('Disconnected client %s', tostring(client))

        fiber.create(function()
            while rf[1] do
                local task = box.space.MegaQueue.index
                                        .status_client:min({ 'work', client })
                if task == nil then
                    break
                end
                if task[STATUS] ~= 'work' then
                    break
                end
                if task[CLIENT] ~= client then
                    break
                end
                self:_task_to_ready(task)
            end
        end)
    end
end

--------- Don't show private methods
local priv = {}
local pub  = {}

for key, m in pairs(mq) do
    if string.match(key, '^_') then
        priv[key] = m
    else
        pub[key] = m
    end
end

setmetatable(pub, { __index = priv })

return pub
