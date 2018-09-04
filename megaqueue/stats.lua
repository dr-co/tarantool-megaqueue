local fiber = require 'fiber'
local log = require 'log'

local stats = {}
local private = {}
local STAT_TIMEOUT      = 6 * 3600

local function as_map(table)
    return setmetatable(table, { __serialize = 'map' })
end


function stats.init(self)
    if self._cleanup ~= nil then
        self._cleanup[1] = false
    end
    self._cleanup = { true, fiber.time() }
    
    fiber.create(function()
        fiber.name('stat-clean')
        local c = self._cleanup
        log.info('MegaQueue: stat cleanup fiber started')
        while c[1] do
            local rm = {}
            for _, t in box.space.MegaQueueStats.index.tube:pairs() do

                local time = t[2].time
                if time == nil then
                    time = c[2]
                end

                if fiber.time() - time > STAT_TIMEOUT then
                    local tube = t[1]

                    local e = box.space.MegaQueue.index.tube_status_pri_id
                            :select(tube, { limit = 1 })
                    if #e == 0 or e[1][2] ~= tube then
                        table.insert(rm, tube)
                    end
                end
            end
            if #rm > 0 then
                log.info('MegaQueue: drop %d old stats records', #rm)
                box.begin()
                for _, tube in pairs(rm) do
                    box.space.MegaQueueStats:delete(tube)
                end
                box.commit()
            end
            fiber.sleep(60)
        end
        log.info('MegaQueue: stat cleanup fiber finished')
    end)
end

function stats.rebuild(self)
    local stat = {}
    for _, t in box.space.MegaQueue.index.id:pairs() do
        local s = t[5]
        local tube = t[2]

        if stat[tube] == nil then
            stat[tube] = {}
        end

        if stat[tube][s] == nil then
            stat[tube][s] = 1
        else
            stat[tube][s] = stat[tube][s] + 1
        end
    end

    for _, s in box.space.MegaQueueStats.index.tube:pairs() do
        if stat[ s[1] ] == nil then
            stat[ s[1] ] = as_map({})
        end

        for k, v in pairs(s[2]) do
            if stat[ s[1] ][ k ] == nil then
                stat[ s[1] ][ k ] = 0
            end
        end
    end


    local count = 0
    box.begin()
    for tube, s in pairs(stat) do
        s.time = fiber.time()
        box.space.MegaQueueStats:replace { tube, s }
        count = count + 1
    end
    box.commit()

    return count
end

function stats.dec(self, tube, key)
    local cnt = self:_fetch(tube)
    if cnt[key] == nil then
        cnt[key] = -1
    else
        cnt[key] = cnt[key] - 1
    end
    self:_store(tube, cnt)
end

function stats.inc(self, tube, key, dec_key)
    if key == dec_key then
        return
    end

    local cnt = self:_fetch(tube)

    if cnt[key] == nil then
        cnt[key] = 1
    else
        cnt[key] = cnt[key] + 1
    end

    if dec_key ~= nil then
        if cnt[dec_key] == nil then
            cnt[dec_key] = -1
        else
            cnt[dec_key] = cnt[dec_key] - 1
        end
    end

    self:_store(tube, cnt)
end




function private._store(self, tube, cnt)
    cnt.time = fiber.time()
    box.space.MegaQueueStats:replace({ tube, cnt })
end

function private._fetch(self, tube)
    local tuple = box.space.MegaQueueStats:get(tube)
    if not tuple then
        return {}
    end
    return tuple[2]
end

setmetatable(stats, { __index = private })

return stats
