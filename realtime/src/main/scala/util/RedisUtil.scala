package util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * @author kylinWang
  * @data 2020/3/26 15:54
  *
  */
object RedisUtil {

    val host = PropertiesUtil.getProperties("config.properties", "redis.host")
    val port = PropertiesUtil.getProperties("config.properties","redis.port").toInt
    //相关的参数
    private val jedisPoolConfig = new JedisPoolConfig()
    jedisPoolConfig.setMaxTotal(100)
    jedisPoolConfig.setMaxIdle(10) //最大空闲
    jedisPoolConfig.setMinIdle(10)//最小空闲
    jedisPoolConfig.setBlockWhenExhausted(true)
    jedisPoolConfig.setMaxWaitMillis(500)
    jedisPoolConfig.setTestOnBorrow(false)
    //JedisPool 连接池
    private val jedisPool = new JedisPool(jedisPoolConfig,host,port)

    //测试连接池
    def getJedisClient ={
        //return jedis
        jedisPool.getResource
    }

}
