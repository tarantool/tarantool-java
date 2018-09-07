time = require('clock').time

box.cfg {
    listen = '0.0.0.0:3301',
}

box.once('init', function()
    local sp = box.schema.space.create('user', { format = {
        { name = 'id',    type = 'integer' },
        { name = 'name',  type = 'string'  },
        { name = 'privs', type = 'any'     },
        { name = 'value', type = 'integer' },
    } })
    sp:create_index('primary', { sequence = true, parts = { 'id' } })

    box.schema.user.create('test_ordin', { password = '2HWRXHfa' })
    box.schema.user.create('test_admin', { password = '4pWBZmLEgkmKK5WP' })

    box.schema.user.grant('test_ordin', 'read,write', 'user')
    box.schema.user.grant('test_admin', 'execute',    'super')
end)

