time = require('clock').time

box.cfg {
    listen = '0.0.0.0:3301',
}

box.once('init', function()
    box.schema.user.grant('guest','read,write,execute,create,drop','universe')
end)

