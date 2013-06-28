require('chai').should()
redis = require 'redis'
RedisQueue = require '../src/index'
Worker = RedisQueue.Worker

describe 'RedisQueue', ->
  describe 'constructor', ->
    it 'can set timeout', (done) ->
      redisQueue = new RedisQueue
        host: '127.0.0.1'
        port: 6379
      , 1
      , (err, q) ->
        return done err if err?
        try
          redisQueue.timeout.should.equal 1
          redisQueue.stop done
        catch e
          done e
    it 'sets the timeout to 60s by default', (done) ->
      redisQueue = new RedisQueue
        host: '127.0.0.1'
        port: 6379
      , (err, q) ->
        return done err if err?
        try
          redisQueue.timeout.should.equal 60
          redisQueue.stop done
        catch e
          done e

    context 'called without password', ->
      it 'can connect if connection details given instead of connection', (done) ->
        redisQueue = new RedisQueue
          host: '127.0.0.1'
          port: 6379
        , (err, q) ->
          return done err if err?
          try
            redisQueue.should.equal q
            redisQueue.conn.connected.should.equal true
            redisQueue.stop done
          catch e
            done e
    context 'called with wrong password', ->
      it 'can not authenticate', (done) ->
        redisQueue = new RedisQueue
          host: '127.0.0.1'
          port: 6379
          password: 'blabla'
        , (err, q) ->
          try
            redisQueue.should.equal q
            redisQueue.conn.connected.should.equal true
            err.should.not.be.null
            redisQueue.stop done
          catch e
            done e

  context 'connected', ->
    redisQueue = null
    pushRedisQueue = null
    beforeEach (done) ->
      client = redis.createClient()
      client.flushall ->
        redisQueue = new RedisQueue client, 1
        pushClient = redis.createClient()
        pushRedisQueue = new RedisQueue pushClient
        done()

    afterEach (done) ->
      redisQueue.stop ->
        pushRedisQueue.stop done

    it 'should receive a pushed job on the monitored queues', (done) ->
      redisQueue.on 'message', (queue, data) ->
        done()
      redisQueue.monitor 'test-queue'
      pushRedisQueue.push 'test-queue', 'this is a test'

    it 'should receive a string payload correctly', (done) ->
      redisQueue.on 'message', (queue, data) ->
        try
          data.should.equal 'this is a test'
          done()
        catch e
          done e
      redisQueue.monitor 'test-queue'
      pushRedisQueue.push 'test-queue', 'this is a test'

    it 'should receive an object payload correctly', (done) ->
      redisQueue.on 'message', (queue, data) ->
        try
          data.message.should.equal 'this is a test'
          done()
        catch e
          done e
      redisQueue.monitor 'test-queue'
      pushRedisQueue.push 'test-queue', {message: 'this is a test'}

    it 'should receive an array payload correctly', (done) ->
      redisQueue.on 'message', (queue, data) ->
        try
          data[0].should.equal 'this is a test'
          done()
        catch e
          done e
      redisQueue.monitor 'test-queue'
      pushRedisQueue.push 'test-queue', ['this is a test']

    it 'should not receive tasks from other queues', (done) ->
      redisQueue.on 'message', (queue, data) ->
        done new Error 'it received message'
      redisQueue.monitor 'test-queue'
      pushRedisQueue.push 'not-test-queue', 'this is a test'
      setTimeout ->
        done()
      , 50

    it 'should not receive other task when the previous is in progress', (done) ->
      called = 0
      redisQueue.on 'message', (queue, data) ->
        called++
        done new Error 'it received task while not done' if called >= 2
      redisQueue.monitor 'test-queue'
      pushRedisQueue.push 'test-queue', 'this is a test'
      pushRedisQueue.push 'test-queue', 'this is an other test'
      setTimeout ->
        done()
      , 50

    it 'should receive other task when the previous is done', (done) ->
      called = 0
      redisQueue.on 'message', (queue, data, callback) ->
        called++
        done() if called >= 2
        callback()
      redisQueue.monitor 'test-queue'
      pushRedisQueue.push 'test-queue', 'this is a test'
      pushRedisQueue.push 'test-queue', 'this is an other test'

    context 'with 2 consumers', ->
      redisQueue2 = null

      beforeEach ->
        redisQueue2 = new RedisQueue redis.createClient(), 1

      it 'should distribute tasks between them', (done) ->
        clientCalled = false
        client2Called = false
        redisQueue.on 'message', (queue, data) ->
          if clientCalled
            done new Error 'the same client received both tasks'
          else
            clientCalled = true
            if client2Called
              redisQueue2.stop done
        redisQueue2.on 'message', (queue, data) ->
          if client2Called
            done new Error 'the same client received both tasks'
          else
            client2Called = true
            if clientCalled
              redisQueue2.stop done
        redisQueue.monitor 'test-queue'
        redisQueue2.monitor 'test-queue'
        pushRedisQueue.push 'test-queue', 'this is a test'
        pushRedisQueue.push 'test-queue', 'this is an other test'

      it 'should not send task for disconnected client', (done) ->
        called = 0
        redisQueue.on 'message', (queue, data, callback) ->
          called++
          done() if called >= 2
          callback()
        redisQueue2.on 'message', (queue, data) ->
          done new Error 'it has received tasks after disconnect'
        redisQueue.monitor 'test-queue'
        redisQueue2.monitor 'test-queue'
        redisQueue2.stop ->
          pushRedisQueue.push 'test-queue', 'this is a test'
          pushRedisQueue.push 'test-queue', 'this is an other test'

  describe 'Worker', ->
    describe 'constructor', ->
      context 'called without password', ->
        it 'can connect if connection details given instead of connection', (done) ->
          worker = new Worker 'test-queue',
            test: ->
          ,
            host: '127.0.0.1'
            port: 6379
          , 1
          , (err, w) ->
            return done err if err?
            try
              worker.should.equal w
              worker.queue.conn.connected.should.be.true
              worker.stop done
            catch e
              done e
      context 'called with wrong password', ->
        it 'can not authenticate', (done) ->
          worker = new Worker 'test-queue',
            test: ->
          ,
            host: '127.0.0.1'
            port: 6379
            password: 'blabla'
          , 1
          , (err, w) ->
            try
              worker.queue.conn.connected.should.be.true
              err.should.not.be.null
              worker.stop done
            catch e
              done e
      context 'connected', ->
        redisQueue = null

        beforeEach (done) ->
          client = redis.createClient()
          client.flushall ->
            redisQueue = new RedisQueue client
            done()
        afterEach (done) ->
          redisQueue.stop done

        it 'can perform an enqueued task', (done) ->
          worker = new Worker 'test-queue',
            test: (data) ->
              try
                data.should.equal 'something'
              catch e
                done e
              worker.stop done
          , redis.createClient()
          , 1
          redisQueue.enqueue 'test-queue', 'test', ['something']

        it 'emits job if task is received', (done) ->
          worker = new Worker 'test-queue',
            test: (data, callback) ->
          , redis.createClient()
          , 1
          worker.on 'job', (w, queueKeys, data) ->
            try
              worker.should.equal w
              queueKeys.should.equal 'test-queue'
              data[0].should.equal 'test'
              data[1][0].should.equal 'something'
              worker.stop done
            catch e
              done e
          redisQueue.enqueue 'test-queue', 'test', ['something']

        it 'emits success if task is done', (done) ->
          worker = new Worker 'test-queue',
            test: (data, callback) ->
              callback()
          , redis.createClient()
          , 1
          worker.on 'success', (w, queueKeys, data) ->
            try
              worker.should.equal w
              queueKeys.should.equal 'test-queue'
              data[0].should.equal 'test'
              data[1][0].should.equal 'something'
              worker.stop done
            catch e
              done e
          redisQueue.enqueue 'test-queue', 'test', ['something']

        it 'emits error if callback is called with non-null', (done) ->
          worker = new Worker 'test-queue',
            test: (data, callback) ->
              callback false
          , redis.createClient()
          , 1
          worker.on 'error', (w, err, queueKeys, data) ->
            try
              worker.should.equal w
              queueKeys.should.equal 'test-queue'
              data[0].should.equal 'test'
              data[1][0].should.equal 'something'
              err.should.equal false
              worker.stop done
            catch e
              done e
          redisQueue.enqueue 'test-queue', 'test', ['something']
