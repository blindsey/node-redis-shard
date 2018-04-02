const assert = require('assert');
const HashRing = require('hashring');
const redis = require('redis');
const step = require('step');
const _ = require('lodash');
const async = require('async');

module.exports = function RedisShard(options) {
  assert(!!options, 'options must be an object');
  assert(Array.isArray(options.servers), 'servers must be an array');

  const self = {};
  const clients = {};
  options.servers.forEach((server) => {
    const fields = server.split(/:/);
    const clientOptions = options.clientOptions || {};
    const client = redis.createClient(parseInt(fields[1], 10), fields[0], clientOptions);
    if (options.database) {
      client.select(options.database, () => {});
    }
    if (options.password) {
      client.auth(options.password);
    }
    clients[server] = client;
  });

  const servers = {};
  for (const key in clients) {
    servers[key] = 1; // balanced ring for now
  }
  self.ring = new HashRing(servers);

  // All of these commands have 'key' as their first parameter
  const SHARDABLE = [
    'append', 'bitcount', 'blpop', 'brpop', 'debug object', 'decr', 'decrby', 'del', 'dump', 'exists', 'expire',
    'expireat', 'get', 'getbit', 'getrange', 'getset', 'hdel', 'hexists', 'hget', 'hgetall', 'hincrby',
    'hincrbyfloat', 'hkeys', 'hlen', 'hmget', 'hmset', 'hset', 'hsetnx', 'hvals', 'incr', 'incrby', 'incrbyfloat',
    'lindex', 'linsert', 'llen', 'lpop', 'lpush', 'lpushx', 'lrange', 'lrem', 'lset', 'ltrim', 'move',
    'persist', 'pexpire', 'pexpireat', 'psetex', 'pttl', 'rename', 'renamenx', 'restore', 'rpop', 'rpush', 'rpushx',
    'sadd', 'scard', 'sdiff', 'set', 'setbit', 'setex', 'setnx', 'setrange', 'sinter', 'sismember', 'smembers',
    'sort', 'spop', 'srandmember', 'srem', 'strlen', 'sunion', 'ttl', 'type', 'watch', 'zadd', 'zcard', 'zcount',
    'zincrby', 'zrange', 'zrangebyscore', 'zrank', 'zrem', 'zremrangebyrank', 'zremrangebyscore', 'zrevrange',
    'zrevrangebyscore', 'zrevrank', 'zscore'
  ];
  SHARDABLE.forEach((command) => {
    self[command] = function () {
      const node = self.ring.get(arguments[0]);
      const client = clients[node];
      client[command](...arguments);
    };
  });

  // mget
  self.mget = function () {
    const keys = _.first(arguments);
    const callback = _.last(arguments);
    const mapping = _.reduce(keys, (acc, key) => {
      const node = self.ring.get(key);
      if (_.has(acc, node)) {
        acc[node].push(key);
      } else {
        acc[node] = [key];
      }
      return acc;
    }, {});
    const nodes = _.keys(mapping);
    async.map(nodes, (node, next) => {
      const keys = mapping[node];
      const client = clients[node];
      client.mget(keys, next);
    }, (err, results) => {
      if (err) {
        return callback(err);
      }
      const keyHash = _.reduce(nodes, (acc, node, index) => {
        const keys = mapping[node];
        const values = _.get(results, [index], []);
        return _.assign(acc, _.zipObject(keys, values));
      }, {});
      return callback(err, _.map(keys, key => keyHash[key]));
    });
  };

  // No key parameter to shard on - throw Error
  const UNSHARDABLE = [
    'auth', 'bgrewriteaof', 'bgsave', 'bitop', 'brpoplpush', 'client kill', 'client list', 'client getname',
    'client setname', 'config get', 'config set', 'config resetstat', 'dbsize', 'debug segfault', 'discard',
    'echo', 'eval', 'evalsha', 'exec', 'flushall', 'flushdb', 'info', 'keys', 'lastsave', 'migrate', 'monitor',
    'mset', 'msetnx', 'multi', 'object', 'ping', 'psubscribe', 'publish', 'punsubscribe', 'quit', 'randomkey',
    'rpoplpush', 'save', 'script exists', 'script flush', 'script kill', 'script load', 'sdiffstore', 'select',
    'shutdown', 'sinterstore', 'slaveof', 'slowlog', 'smove', 'subscribe', 'sunionstore', 'sync', 'time',
    'unsubscribe', 'unwatch', 'zinterstore', 'zunionstore'
  ];
  UNSHARDABLE.forEach((command) => {
    self[command] = function () {
      throw new Error(`${command} is not shardable`);
    };
  });

  // This is the tricky part - pipeline commands to multiple servers
  self.multi = function Multi() {
    const self = {};
    const multis = {};
    const interlachen = [];

    // Setup chainable shardable commands
    SHARDABLE.forEach((command) => {
      self[command] = function () {
        const node = self.ring.get(arguments[0]);
        let multi = multis[node];
        if (!multi) {
          multi = multis[node] = clients[node].multi();
        }
        interlachen.push(node);
        multi[command](...arguments);
        return self;
      };
    });

    UNSHARDABLE.forEach((command) => {
      self[command] = function () {
        throw new Error(`${command} is not supported`);
      };
    });

    // Exec the pipeline and interleave the results
    self.exec = function (callback) {
      const nodes = Object.keys(multis);
      step(
        function run() {
          const group = this.group();
          nodes.forEach((node) => {
            multis[node].exec(group());
          });
        },
        (error, groups) => {
          if (error) { return callback(error); }
          assert(nodes.length === groups.length, 'wrong number of responses');
          const results = [];
          interlachen.forEach((node) => {
            const index = nodes.indexOf(node);
            assert(groups[index].length > 0, `${node} is missing a result`);
            results.push(groups[index].shift());
          });
          callback(null, results);
        }
      );
    };
    return self; // Multi()
  };


  self.on = function (event, listener) {
    options.servers.forEach((server) => {
      clients[server].on(event, function () {
        // append server as last arg passed to listener
        const args = Array.prototype.slice.call(arguments).concat(server);
        listener(...args);
      });
    });
  };

  // Note: listener will fire once per shard, not once per cluster
  self.once = function (event, listener) {
    options.servers.forEach((server) => {
      clients[server].once(event, function () {
        // append server as last arg passed to listener
        const args = Array.prototype.slice.call(arguments).concat(server);
        listener(...args);
      });
    });
  };

  return self; // RedisShard()
};
