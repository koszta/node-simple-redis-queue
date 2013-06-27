EventEmitter = require('events').EventEmitter

class RedisQueueError extends Error

class RedisQueue extends EventEmitter
  constructor: (@conn) ->

  push: (type, payload) ->
    @conn.lpush type, JSON.stringify payload

  monitor: (keysToMonitor...) ->
    @conn.brpop keysToMonitor..., 0, (err, replies) =>
      try
        return @emit 'error', err if err?
        if replies.length != 2
          return @emit 'error',
          new RedisQueueError "Bad number of replies from redis #{replies.length}"

        @emit 'message', replies[0], JSON.parse(replies[1]), (err) =>
          @emit 'error', err if err?
          @monitor(keysToMonitor...)

  clear: (keysToClear...) ->
    @conn.del keysToClear...

module.exports = RedisQueue
