var EventEmitter, RedisQueue, RedisQueueError, Worker, redis,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  __slice = [].slice;

redis = require('redis');

EventEmitter = require('events').EventEmitter;

RedisQueueError = (function(_super) {

  __extends(RedisQueueError, _super);

  function RedisQueueError() {
    return RedisQueueError.__super__.constructor.apply(this, arguments);
  }

  return RedisQueueError;

})(Error);

RedisQueue = (function(_super) {

  __extends(RedisQueue, _super);

  function RedisQueue(conn, callback) {
    var _this = this;
    if (callback == null) {
      callback = function() {};
    }
    if (conn.connected) {
      this.conn = conn;
      callback(null, this);
    } else {
      this.conn = redis.createClient(conn.port, conn.host);
      this.conn.once('connect', function() {
        if (conn.password != null) {
          return _this.conn.auth(conn.password, function(err) {
            return callback(err, _this);
          });
        } else {
          return callback(null, _this);
        }
      });
    }
  }

  RedisQueue.prototype.push = function(queueKeys, payload) {
    return this.conn.lpush(queueKeys, JSON.stringify(payload));
  };

  RedisQueue.prototype.enqueue = function(queueKeys, taskType, payload) {
    return this.push(queueKeys, [taskType, payload]);
  };

  RedisQueue.prototype.monitor = function() {
    var keysToMonitor, _ref,
      _this = this;
    keysToMonitor = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
    return (_ref = this.conn).brpop.apply(_ref, __slice.call(keysToMonitor).concat([0], [function(err, replies) {
      try {
        if (err != null) {
          return _this.emit('error', err);
        }
        if (replies.length !== 2) {
          return _this.emit('error', new RedisQueueError("Bad number of replies from redis " + replies.length));
        }
        return _this.emit('message', replies[0], JSON.parse(replies[1]), function(err) {
          if (err != null) {
            _this.emit('error', err);
          }
          return _this.monitor.apply(_this, keysToMonitor);
        });
      } catch (_error) {}
    }]));
  };

  RedisQueue.prototype.clear = function() {
    var queueKeys, _ref;
    queueKeys = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
    return (_ref = this.conn).del.apply(_ref, queueKeys);
  };

  RedisQueue.prototype.stop = function(callback) {
    if (callback == null) {
      callback = function() {};
    }
    this.conn.end();
    return this.conn.stream.once('close', function() {
      return callback();
    });
  };

  return RedisQueue;

})(EventEmitter);

Worker = (function() {

  function Worker(queueKeys, tasks, conn, callback) {
    var _this = this;
    this.queueKeys = queueKeys;
    this.tasks = tasks;
    if (callback == null) {
      callback = function() {};
    }
    this.queue = new RedisQueue(conn, function(err) {
      if (err != null) {
        return callback(err);
      }
      _this.queue.on('message', function(queueKeys, data, callback) {
        try {
          return _this.perform(data, callback);
        } catch (e) {
          return callback(e);
        }
      });
      _this.queue.monitor(_this.queueKeys);
      return callback(null, _this);
    });
  }

  Worker.prototype.stop = function(callback) {
    if (callback == null) {
      callback = function() {};
    }
    return this.queue.stop(callback);
  };

  Worker.prototype.perform = function(data, callback) {
    var payload, task, _ref;
    task = data[0];
    payload = data[1] || [];
    payload.push(callback);
    return (_ref = this.tasks)[task].apply(_ref, payload);
  };

  return Worker;

})();

RedisQueue.Worker = Worker;

module.exports = RedisQueue;
