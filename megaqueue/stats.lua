local function get_cnt(tube)
    local res = box.space.MegaQueueStats:get(tube)
    if res == nil then
        return {}
    end

    return res[2]
end

local function store_cnt(tube, cnt)
    box.space.MegaQueueStats:replace({ tube, cnt })
end

local stats = {
    inc     = function(tube, name, add)
        if (add == nil) then
            add = 1
        end
        local c = get_cnt(tube)
        
        if c[name] == nil then
            c[name] = 0
        end
        
        c[name] = c[name] + add
        store_cnt(tube, c)
    end,

    dec     = function(tube, name, sub)
        if (sub == nil) then
            sub = 1
        end
        
        local c = get_cnt(tube)
        
        if c[name] == nil then
            c[name] = 0
        end
        
        c[name] = c[name] - sub
        store_cnt(tube, c)
    end,


    inc_dec = function(tube, name_inc, name_dec, add)
        if (add == nil) then
            add = 1
        end

        require('log').info('%s %s -> %s: %s', tube, name_inc, name_dec, add)

        local c = get_cnt(tube)
        if c[name_inc] == nil then
            c[name_inc] = 0
        end
        if c[name_dec] == nil then
            c[name_dec] = 0
        end
        c[name_dec] = c[name_dec] - add
        c[name_inc] = c[name_inc] + add
        store_cnt(tube, c)
    end,
}

return stats
