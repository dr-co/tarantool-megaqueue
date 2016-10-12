box.cfg {
    listen      = 1234    
}

box.schema.user.create('queue_user', { password = 'queue_password', if_not_exists = true })
box.schema.user.grant('queue_user', 'read,write,execute', 'universe', nil, { if_not_exists = true })

_G.queue = require 'megaqueue'
queue:init()
