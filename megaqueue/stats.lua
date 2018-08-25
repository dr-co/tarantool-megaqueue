local private = {}

local stats = {}

local function as_map(table)
    return setmetatable(table, { __serialize = 'map' })
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
