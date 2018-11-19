box.cfg {
    listen = 3301,
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

    box.schema.user.grant('test_ordin', 'read,write', 'space', 'user')
    box.schema.user.grant('test_admin', 'super')
end)

-- Java has no internal support for unix domain sockets,
-- so we will use tcp for console communication.
console = require('console')
console.listen(3313)
