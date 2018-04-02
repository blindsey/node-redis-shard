const RedisServer = require('redis-server');
const async = require('async');
const _ = require('lodash');

const RedisShard = require('./index');

const PORT = 7000;
const NUM = 5;

async.times(NUM, (index, next) => {
  const server = new RedisServer(PORT + index);
  server.open(next);
}, (err) => {
  const options = {
    servers: _.map(_.times(NUM, n => `127.0.0.1:${PORT + n}`))
  };
  const redis = new RedisShard(options);
  const KEY_NUM = 20;

  async.times(KEY_NUM, (n, next) => redis.set(`foo${n}`, `bar${n}`, next), (err) => {
    const keys = _.times(KEY_NUM, n => `foo${n}`);
    redis.mget(keys, (err, values) => {
      //      console.log(keys, values);
      if (err || keys.length != values.length) {
        return console.log('Error is', err || 'length mismatch');
      }
      console.log('Tests successful');
    });
  });
});

