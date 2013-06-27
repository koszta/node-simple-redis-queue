redis = require 'redis'
EventEmitter = require('events').EventEmitter

class RedisQueueError extends Error

class RedisQueue extends EventEmitter
  constructor: (conn, callback = ->) ->
    if conn.connected
      @conn = conn
      callback null, this
    else
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
    @conn.brpop keysToMonitor..., 0, (err, replies) =>
      try
        return @emit 'error', err if err?
        if replies.length != 2
          return @emit 'error',
          new RedisQueueError "Bad number of replies from redis #{replies.length}"

        @emit 'message', replies[0], JSON.parse(replies[1]), (err) =>
          @emit 'error', err if err?
          @monitor keysToMonitor...

  clear: (queueKeys...) ->
    @conn.del queueKeys...

  stop: (callback = ->) ->
    @conn.end()
    @conn.stream.once 'close', ->
      # sends false as err argument, ignore it
      callback()

class Worker
  constructor: (@queueKeys, @tasks, conn, callback = ->) ->
    @queue = new RedisQueue conn, (err) =>
      return callback err if err?
      @queue.on 'message', (queueKeys, data, callback) =>
        try
          @perform data, callback
        catch e
          callback e
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
