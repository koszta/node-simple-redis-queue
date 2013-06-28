redis = require 'redis'
EventEmitter = require('events').EventEmitter

class RedisQueueError extends Error

class RedisQueue extends EventEmitter
  constructor: (conn, callback = ->) ->
    @quiting = false
    @timeout = 60
    if conn.connected
      @conn = conn
      callback null, this
    else
      @timeout = conn.timeout if conn.timeout?
      @conn = redis.createClient conn.port, conn.host
      @conn.once 'connect', =>
        if conn.password?
          @conn.auth conn.password, (err) =>
            callback err, this
        else
          callback null, this

  push: (queueKeys, payload) ->
    @conn.lpush queueKeys, JSON.stringify payload

  enqueue: (queueKeys, taskType, payload) ->
    @push queueKeys, [taskType, payload]

  monitor: (keysToMonitor...) ->
    return if @quiting
    @conn.brpop keysToMonitor..., @timeout, (err, replies) =>
      try
        return @emit 'error', err if err?
        return @monitor keysToMonitor... unless replies?
        return @conn.lpush replies... if @quiting
        if replies.length != 2
          return @emit 'error',
          new RedisQueueError "Bad number of replies from redis #{replies.length}"

        queueKeys = replies[0]
        data = JSON.parse(replies[1])
        @emit 'message', queueKeys, data, (err) =>
          if err?
            @emit 'error', err, queueKeys, data
          else
            @emit 'success', queueKeys, data
          @monitor keysToMonitor...

  clear: (queueKeys...) ->
    @conn.del queueKeys...

  stop: (callback = ->) ->
    @quiting = true
    @conn.once 'end', callback
    @conn.quit()

class Worker extends EventEmitter
  constructor: (@queueKeys, @tasks, conn, callback = ->) ->
    @queue = new RedisQueue conn, (err) =>
      return callback err if err?
      @queue.on 'message', (queueKeys, data, callback) =>
        @emit 'job', this, arguments...
        try
          @perform data, callback
        catch e
          callback e
      @queue.on 'error', =>
        @emit 'error', this, arguments...
      @queue.on 'success', =>
        @emit 'success', this, arguments...
      @queue.monitor @queueKeys
      callback null, this

  stop: (callback = ->) ->
    @queue.stop callback

  perform: (data, callback) ->
    task = data[0]
    payload = data[1] || []
    payload.push callback
    @tasks[task] payload...

RedisQueue.Worker = Worker

module.exports = RedisQueue
