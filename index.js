var assert = require('assert');
var HashRing = require('hashring');
var redis = require('redis');
var step = require('step');

module.exports = function RedisShard(options) {
  assert(!!options, "options must be an object");
  assert(Array.isArray(options.servers), "servers must be an array");

  var self = {};
  var clients = {};
  options.servers.forEach(function(server) {
    var fields = server.split(/:/);
    var clientOptions = options.clientOptions || {};
    var client = redis.createClient(parseInt(fields[1], 10), fields[0], clientOptions);
    if ( options.database ) {
      client.select(options.database, function(){});
    }
    if ( options.password ) {
      client.auth(options.password);
    }
    clients[server] = client;
  });

  var servers = {};
  for (var key in clients) {
    servers[key] = 1; // balanced ring for now
  }
  self.ring = new HashRing(servers);

  // All of these commands have 'key' as their first parameter
  var SHARDABLE = [
    "append", "bitcount", "blpop", "brpop", "debug object", "decr", "decrby", "del", "dump", "exists", "expire",
    "expireat", "get", "getbit", "getrange", "getset", "hdel", "hexists", "hget", "hgetall", "hincrby",
    "hincrbyfloat", "hkeys", "hlen", "hmget", "hmset", "hset", "hsetnx", "hvals", "incr", "incrby", "incrbyfloat",
    "lindex", "linsert", "llen", "lpop", "lpush", "lpushx", "lrange", "lrem", "lset", "ltrim", "mget", "move",
    "persist", "pexpire", "pexpireat", "psetex", "pttl", "rename", "renamenx", "restore", "rpop", "rpush", "rpushx",
    "sadd", "scard", "sdiff", "set", "setbit", "setex", "setnx", "setrange", "sinter", "sismember", "smembers",
    "sort", "spop", "srandmember", "srem", "strlen", "sunion", "ttl", "type", "watch", "zadd", "zcard", "zcount",
    "zincrby", "zrange", "zrangebyscore", "zrank", "zrem", "zremrangebyrank", "zremrangebyscore", "zrevrange",
    "zrevrangebyscore", "zrevrank", "zscore"
  ];
  SHARDABLE.forEach(function(command) {
    self[command] = function() {
      var node = self.ring.get(arguments[0]);
      var client = clients[node];
      client[command].apply(client, arguments);
    };
  });

  // No key parameter to shard on - throw Error
  var UNSHARDABLE = [
    "auth", "bgrewriteaof", "bgsave", "bitop", "brpoplpush", "client kill", "client list", "client getname",
    "client setname", "config get", "config set", "config resetstat", "dbsize", "debug segfault", "discard",
    "echo", "eval", "evalsha", "exec", "flushall", "flushdb", "info", "keys", "lastsave", "migrate", "monitor",
    "mset", "msetnx", "object", "ping", "psubscribe", "publish", "punsubscribe", "quit", "randomkey",
    "rpoplpush", "save", "script exists", "script flush", "script kill", "script load", "sdiffstore", "select",
    "shutdown", "sinterstore", "slaveof", "slowlog", "smove", "subscribe", "sunionstore", "sync", "time",
    "unsubscribe", "unwatch", "zinterstore", "zunionstore"
  ];
  UNSHARDABLE.forEach(function(command) {
    self[command] = function() {
      throw new Error(command + ' is not shardable');
    };
  });

  // This is the tricky part - pipeline commands to multiple servers
  self.multi = function Multi() {

    var self = {};
    var multis = {};
    var interlachen = [];

    // Setup chainable shardable commands
    SHARDABLE.forEach(function(command) {
      self[command] = function() {
        var node = self.ring.get(arguments[0]);
        var multi = multis[node];
        if (!multi) {
          multi = multis[node] = clients[node].multi();
        }
        interlachen.push(node);
        multi[command].apply(multi, arguments);
        return self;
      };
    });

    UNSHARDABLE.forEach(function(command) {
      self[command] = function() {
        throw new Error(command + " is not supported");
      };
    });

    // Exec the pipeline and interleave the results
    self.exec = function(callback) {
      var nodes = Object.keys(multis);
      step(
        function run() {
          var group = this.group();
          nodes.forEach(function(node) {
            multis[node].exec(group());
          });
        },
        function done(error, groups) {
          if (error) { return callback(error); }
          assert(nodes.length === groups.length, "wrong number of responses");
          var results = [];
          interlachen.forEach(function(node) {
            var index = nodes.indexOf(node);
            assert(groups[index].length > 0, node + " is missing a result");
            results.push(groups[index].shift());
          });
          callback(null, results);
        }
      );
    };
    return self; // Multi()
  };


  self.on = function(event, listener) {
    options.servers.forEach(function(server) {
      clients[server].on(event, function() {
        // append server as last arg passed to listener
        var args = Array.prototype.slice.call(arguments).concat(server);
        listener.apply(undefined, args);
      });
    });
  };

  // Note: listener will fire once per shard, not once per cluster
  self.once = function(event, listener) {
    options.servers.forEach(function(server) {
      clients[server].once(event, function() {
        // append server as last arg passed to listener
        var args = Array.prototype.slice.call(arguments).concat(server);
        listener.apply(undefined, args);
      });
    });
  }

  return self; // RedisShard()
};
