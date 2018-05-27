redis-shard-optimized
=====================

[![CircleCI](https://circleci.com/gh/ppsreejith/node-redis-shard.svg?style=svg)](https://circleci.com/gh/ppsreejith/node-redis-shard)

A consistent sharding library for redis in node.

This project improves over the parent project by consistently distributing mget and mset over the sharded redis instances. (The parent implementation sends mget and mset to a single instance).

    $ npm install redis-shard-optimized

    var RedisShard = require('redis-shard-optimized');
    var options = { servers: [ '127.0.0.1:6379', '127.0.0.1:6479' ], database : 1, password : 'redis4pulseLocker' };
    var redis = new RedisShard(options);

    // SINGLE
    redis.set('foo', 'bar', console.log);
    redis.get('foo', console.log);

    // SINGLE (Multi key commands)
    redis.mset(['key1', 'val1', 'key2', 'val2', 'key3', 'val3'], console.log);
    redis.mget(['key1', 'key2', 'key3'], console.log);

    // MULTI
    var multi = redis.multi();
    multi.set('foo', 'bar').set('bah', 'baz').expire('foo', 3600).expire('bah', 3600);
    multi.exec(console.log);

Options
-------

The constructor accepts an object containing the following options:

- `servers` (required) - An array of Redis servers (e.g. `'127.0.0.1:6379'`) to connect to
- `database` - Redis database to select
- `password` - Password for authentication
- `clientOptions` - Options object to be passed to each Redis client

Tests
-----

To run tests, use

```
npm run tests
```