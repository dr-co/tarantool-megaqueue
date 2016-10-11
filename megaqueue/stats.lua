local private = {}

local stats = {}


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
