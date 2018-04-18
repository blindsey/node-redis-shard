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
  const KEY_NUM = 10;

  const keys = _.times(KEY_NUM, n => `foo${n}`);
  const values = _.times(KEY_NUM, n => `bar${n}`);
  const msetArgs = _
    .chain(keys)
    .zip(values)
    .fromPairs()
    .value();

  async.auto({
    set: callback =>  { _.map(msetArgs,(key,value) => redis.set(key,value,() => {})); callback();},
    get: ['set', callback => redis.keys("bar*", callback)]
  }, (err, { set, get }) => {
    if (err) {
      throw err;
    } else if (!_.isEmpty(_.difference(get,values))) {
      throw 'Keys response doesn\'t match values';
    } else {
      console.log('Success. All tests passed.');
      process.exit(0);
    }
  });
});