redis-shard
===========

A consistent sharding library for redis in node

    $ npm install redis-shard

    var RedisShard = require('redis-shard');
    var options = { servers: [ '127.0.0.1:6379', '127.0.0.1:6479' ], database : 1, password : 'redis4pulseLocker' };
    var redis = new RedisShard(options);

    // SINGLE
    redis.set('foo', 'bar', console.log);
    redis.get('foo', console.log);

    // MULTI
    var multi = redis.multi();
    multi.set('foo', 'bar').set('bah', 'baz').expire('foo', 3600).expire('bah', 3600);
    multi.exec(console.log);
