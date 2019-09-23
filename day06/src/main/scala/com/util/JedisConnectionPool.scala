package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Redis连接
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig()

  config.setMaxTotal(20)

  config.setMaxIdle(10)

  private val pool = new JedisPool(config,"node4",6379,10000,"123")

  def getConnection():Jedis={
    pool.getResource
  }

}
