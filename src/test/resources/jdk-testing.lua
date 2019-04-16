box.cfg {
    listen = os.getenv('LISTEN') or 3301,
    replication = os.getenv('MASTER') and string.split(os.getenv('MASTER'), ";") or nil,
    replication_timeout = tonumber(os.getenv('REPLICATION_TIMEOUT')),
}

box.once('init', function()
    box.schema.user.create('test_admin', { password = '4pWBZmLEgkmKK5WP' })
    box.schema.user.grant('test_admin', 'super')
end)

-- Java has no internal support for unix domain sockets,
-- so we will use tcp for console communication.
console = require('console')
console.listen(os.getenv('ADMIN') or 3313)
