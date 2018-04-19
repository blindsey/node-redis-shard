const RedisServer = require('redis-server');
const async = require('async');
const _ = require('lodash');

const RedisShard = require('./index');

const PORT = 7000;
const NUM = 5;
const TESTS = {};

TESTS.keysCommandTests = ({KEY_NUM, redis}, callback) => {
  const keys = _.times(KEY_NUM, n => `fooregexx${n}`);
  const values = _.times(KEY_NUM, n => `barregexx${n}`);

  const keys2 = _.times(KEY_NUM, n => `fooregex${n}`);
  const values2 = _.times(KEY_NUM, n => `barregex${n}`);

  const allKeys = _.concat(keys, keys2);
  const allValues = _.concat(values, values2);

  const msetArgs = _
    .chain(allKeys)
    .zip(allValues)
    .flatten()
    .value();

  const regex1 = 'fooregexx*'; // meant to match keys
  const regex2 = 'fooregex*'; // meant to match allKeys

  async.auto({
    mset: callback => redis.mset(msetArgs, callback),
    keys1: ['mset', callback => redis.keys(regex1, callback)],
    keys2: ['mset', callback => redis.keys(regex2, callback)],
  }, (err, { mset, keys1, keys2 }) => {
    if (err) {
      return callback(err);
    }
    if (mset !== 'OK' ) {
      return callback('mset was not successful');
    }

    const sortedKeys1 = _.sortBy(keys);
    const sortedKeys2 = _.sortBy(allKeys);

    const retrievedKeys1 = _.sortBy(keys1);
    const retrievedKeys2 = _.sortBy(keys2);

    if (!_.isEqual(sortedKeys1, retrievedKeys1) || !_.isEqual(sortedKeys2, retrievedKeys2)) {
      return callback('Keys response doesn\'t match values');
    }
    return callback();
  });
};

TESTS.multiSetGetCommandTests = ({KEY_NUM, redis}, callback) => {
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
    if (err) {
      return callback(err);
    }
    if (mset !== 'OK') {
      return callback('mset was not successful');
    }
    if (!_.isEqual(mget, values)) {
      return callback('Mget response doesn\'t match values');
    }
    return callback();
  });
}

async.times(NUM, (index, next) => {
  const server = new RedisServer(PORT + index);
  server.open(next);
}, (err) => {
  const options = {
    servers: _.map(_.times(NUM, n => `127.0.0.1:${PORT + n}`))
  };
  const redis = new RedisShard(options);
  const KEY_NUM = 1000;

  const args = { KEY_NUM, redis };

  const asyncArgs = _.reduce(
    TESTS,
    (acc, value, key) => {
      acc[key] = _.partial(value, args);
      return acc;
    },
    {}
  );

  async.auto(asyncArgs, (err) => {
    if (err) {
      throw err;
    }
    console.log('Success. All tests passed.');
    process.exit(0);
  });
});