local SCH_KEY       = 'MegaQueueDbVersion'

local log = require 'log'
local migrations = {}
migrations.list = {
    {
        up  = function()
            log.info('First start of megaqueue detected')
        end
    },
    {
        description = 'Create main MegaQueue space',
        up  = function()
            box.schema.space.create(
                'MegaQueue',
                {
                    engine      = 'memtx',
                    format  = {
                        {                           -- #1
                            ['name']    = 'id',
                            ['type']    = 'unsigned',
                        },

                        {                           -- #2
                            ['name']    = 'tube',
                            ['type']    = 'str',
                        },

                        {                           -- #3
                            ['name']    = 'pri',
                            ['type']    = 'unsigned',
                        },

                        {                           -- #4
                            ['name']    = 'domain',
                            ['type']    = 'str',
                        },

                        {                           -- #5
                            ['name']    = 'status',
                            ['type']    = 'str',
                        },

                        {                           -- #6
                            ['name']    = 'event',
                            ['type']    = 'number',
                        },

                        {                           -- #7
                            ['name']    = 'client',
                            ['type']    = 'unsigned',
                        },

                        {                           -- #8
                            ['name']    = 'options',
                            ['type']    = '*',
                        },
                        
                        {                           -- #9
                            ['name']    = 'data',
                            ['type']    = '*',
                        },
                    }
                }
            )
        end
    },

    {
        description = 'MegaQueue: main space primary index',
        up = function()
            box.space.MegaQueue:create_index(
                'id',
                {
                    unique  = true,
                    type    = 'tree',
                    parts   = { 1, 'unsigned' }
                }
            )
        end
    },

    {
        description = 'MegaQueue: create domain index',
        up  = function()
            box.space.MegaQueue:create_index(
                'tube_domain_status',
                {
                    unique  = false,
                    type    = 'tree',
                    parts   = { 2, 'str', 4, 'str', 5, 'str' }
                }
            )
        end
    },

    {
        description = 'MegaQueue: create event index',
        up  = function()
            box.space.MegaQueue:create_index(
                'event',
                {
                    unique  = false,
                    type    = 'tree',
                    parts   = { 6, 'number' }
                }
            )
        end
    },

    {
        description = 'MegaQueue: work index',
        up = function()
            box.space.MegaQueue:create_index(
                'tube_status_pri_id',
                {
                    unique  = false,
                    type    = 'tree',
                    parts   = { 2, 'str', 5, 'str', 3, 'unsigned', 1, 'unsigned' }
                }
            )
        end
    },
    {
        description = 'MegaQueue: status index',
        up = function()
            box.space.MegaQueue:create_index(
                'status_client',
                {
                    unique  = false,
                    type    = 'tree',
                    parts   = { 5, 'str', 7, 'unsigned' }
                }
            )
        end
    },
    
    {
        description = 'Create MegaQueueStats space',
        up  = function()
            box.schema.space.create(
                'MegaQueueStats',
                {
                    engine      = 'memtx',
                    format  = {
                        {                           -- #1
                            ['name']    = 'tube',
                            ['type']    = 'str',
                        },

                        {                           -- #2
                            ['name']    = 'counters',
                            ['type']    = '*',
                        },
                    }
                }
            )
        end
    },
    {
        description = 'MegaQueueStats: main index',
        up = function()
            box.space.MegaQueueStats:create_index(
                'tube',
                {
                    unique  = true,
                    type    = 'hash',
                    parts   = { 1, 'str' }
                }
            )
        end
    },
}


function migrations.upgrade(self, mq)

    local db_version = 0
    local ut = box.space._schema:get(SCH_KEY)
    local version = mq.VERSION

    if ut ~= nil then
        db_version = ut[2]
    end

    local cnt = 0
    for v, m in pairs(migrations.list) do
        if db_version < v then
            local nv = string.format('%s.%03d', version, v)
            log.info('MegaQueue: up to version %s (%s)', nv, m.description)
            m.up(mq)
            box.space._schema:replace{ SCH_KEY, v }
            mq.VERSION = nv
            cnt = cnt + 1
        end
    end
    return cnt
end


return migrations
