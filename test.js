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
  const KEY_NUM = 1000;

  const keys = _.times(KEY_NUM, n => `foo${n}`);
  const values = _.times(KEY_NUM, n => `bar${n}`);
  const msetArgs = _
    .chain(keys)
    .zip(values)
    .flatten()
    .value();

  async.auto({
    mset: callback => redis.mset(msetArgs, callback),
    mget: ['mset', callback => redis.mget(keys, callback)]
  }, (err, { mset, mget }) => {
    console.log('error is', err, mset);
  });
});

