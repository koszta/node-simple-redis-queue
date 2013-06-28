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

  function RedisQueue(conn, timeoutOrCallback, callback) {
    var _this = this;
    if (callback == null) {
      callback = function() {};
    }
    this.quiting = false;
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback;
    }
    if (typeof timeoutOrCallback === 'number') {
      this.timeout = timeoutOrCallback;
    } else {
      this.timeout = 60;
    }
    if (conn.connected) {
      this.conn = conn;
      callback(null, this);
    } else {
      if (conn.timeout != null) {
        this.timeout = conn.timeout;
      }
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
    if (this.quiting) {
      return;
    }
    return (_ref = this.conn).brpop.apply(_ref, __slice.call(keysToMonitor).concat([this.timeout], [function(err, replies) {
      var data, queueKeys, _ref;
      try {
        if (err != null) {
          return _this.emit('error', err);
        }
        if (replies == null) {
          return _this.monitor.apply(_this, keysToMonitor);
        }
        if (_this.quiting) {
          return (_ref = _this.conn).lpush.apply(_ref, replies);
        }
        if (replies.length !== 2) {
          return _this.emit('error', new RedisQueueError("Bad number of replies from redis " + replies.length));
        }
        queueKeys = replies[0];
        data = JSON.parse(replies[1]);
        return _this.emit('message', queueKeys, data, function(err) {
          if (err != null) {
            _this.emit('error', err, queueKeys, data);
          } else {
            _this.emit('success', queueKeys, data);
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
    this.quiting = true;
    this.conn.once('end', callback);
    return this.conn.quit();
  };

  return RedisQueue;

})(EventEmitter);

Worker = (function(_super) {

  __extends(Worker, _super);

  function Worker(queueKeys, tasks, conn, timeoutOrCallback, callback) {
    var args,
      _this = this;
    this.queueKeys = queueKeys;
    this.tasks = tasks;
    if (callback == null) {
      callback = function() {};
    }
    args = [conn];
    if (typeof timeoutOrCallback === 'function') {
      callback = timeoutOrCallback;
    } else if (typeof timeoutOrCallback === 'number') {
      args.push(timeoutOrCallback);
    }
    args.push(function(err) {
      if (err != null) {
        return callback(err);
      }
      _this.queue.on('message', function(queueKeys, data, callback) {
        _this.emit.apply(_this, ['job', _this].concat(__slice.call(arguments)));
        try {
          return _this.perform(data, callback);
        } catch (e) {
          return callback(e);
        }
      });
      _this.queue.on('error', function() {
        return _this.emit.apply(_this, ['error', _this].concat(__slice.call(arguments)));
      });
      _this.queue.on('success', function() {
        return _this.emit.apply(_this, ['success', _this].concat(__slice.call(arguments)));
      });
      _this.queue.monitor(_this.queueKeys);
      return callback(null, _this);
    });
    this.queue = (function(func, args, ctor) {
      ctor.prototype = func.prototype;
      var child = new ctor, result = func.apply(child, args);
      return Object(result) === result ? result : child;
    })(RedisQueue, args, function(){});
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

})(EventEmitter);

RedisQueue.Worker = Worker;

module.exports = RedisQueue;
