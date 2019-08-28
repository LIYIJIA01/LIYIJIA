package logDemo_3.TagUtils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool{
  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(config, "192.168.183.17", 6379, 10000)

  def getConnection(): Jedis = {
    pool.getResource
  }
}
