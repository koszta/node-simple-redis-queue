require('chai').should()
redis = require 'redis'
RedisQueue = require '../src/index'

describe 'RedisQueue', ->
  client = null
  redisQueue = null
  beforeEach (done) ->
    client = redis.createClient()
    client.flushall ->
      redisQueue = new RedisQueue client
      done()

  afterEach (done) ->
    client.end()
    client.stream.on 'close', ->
      done()

  it 'should receive a pushed job on the monitored queues', (done) ->
    redisQueue.on 'message', (queue, data) ->
      done()
    redisQueue.push 'test-queue', 'this is a test'
    redisQueue.monitor 'test-queue'

  it 'should receive a string payload correctly', (done) ->
    redisQueue.on 'message', (queue, data) ->
      try
        data.should.equal 'this is a test'
        done()
      catch e
        done e
    redisQueue.push 'test-queue', 'this is a test'
    redisQueue.monitor 'test-queue'

  it 'should receive an object payload correctly', (done) ->
    redisQueue.on 'message', (queue, data) ->
      try
        data.message.should.equal 'this is a test'
        done()
      catch e
        done e
    redisQueue.push 'test-queue', {message: 'this is a test'}
    redisQueue.monitor 'test-queue'

  it 'should receive an array payload correctly', (done) ->
    redisQueue.on 'message', (queue, data) ->
      try
        data[0].should.equal 'this is a test'
        done()
      catch e
        done e
    redisQueue.push 'test-queue', ['this is a test']
    redisQueue.monitor 'test-queue'

  it 'should not receive tasks from other queues', (done) ->
    redisQueue.on 'message', (queue, data) ->
      done new Error 'it received message'
    redisQueue.push 'not-test-queue', 'this is a test'
    redisQueue.monitor 'test-queue'
    setTimeout ->
      done()
    , 50

  it 'should not receive other task when the previous is in progress', (done) ->
    called = 0
    redisQueue.on 'message', (queue, data) ->
      called++
      done new Error 'it received task while not done' if called >= 2
    redisQueue.push 'test-queue', 'this is a test'
    redisQueue.push 'test-queue', 'this is an other test'
    redisQueue.monitor 'test-queue'
    setTimeout ->
      done()
    , 50

  it 'should receive other task when the previous is done', (done) ->
    called = 0
    redisQueue.on 'message', (queue, data, callback) ->
      called++
      done() if called >= 2
      callback()
    redisQueue.push 'test-queue', 'this is a test'
    redisQueue.push 'test-queue', 'this is an other test'
    redisQueue.monitor 'test-queue'
    setTimeout ->
      done()
    , 50

  context 'with 2 consumers', ->
    client2 = null
    redisQueue2 = null

    beforeEach (done) ->
      client2 = redis.createClient()
      client2.flushall ->
        redisQueue2 = new RedisQueue client2
        done()

    afterEach (done) ->
      client2.end()
      client2.stream.on 'close', ->
        done()

    it 'should distribute tasks between them', (done) ->
      clientCalled = false
      client2Called = false
      redisQueue.on 'message', (queue, data) ->
        if clientCalled
          done new Error 'the same client received both tasks'
        else
          clientCalled = true
          if client2Called
            done()
      redisQueue2.on 'message', (queue, data) ->
        if client2Called
          done new Error 'the same client received both tasks'
        else
          client2Called = true
          if clientCalled
            done()
      redisQueue.push 'test-queue', 'this is a test'
      redisQueue.push 'test-queue', 'this is an other test'
      redisQueue.monitor 'test-queue'
      redisQueue2.monitor 'test-queue'