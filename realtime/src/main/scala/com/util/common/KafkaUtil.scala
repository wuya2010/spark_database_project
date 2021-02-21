package com.util.common

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * @author kylinWang
  * @data 2020/3/26 15:53
  *
  */
object KafkaUtil {

    def getKafkaStream(ssc:StreamingContext,topic:String)={
      val params:Map[String,String] = Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getProperties("config.properties", "kafka.broker.list"),
        ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.getProperties("config.properties", "kafka.group")
      )

      KafkaUtils.createDirectStream(
        ssc,params,Set(topic)
      )
    }

  def main(args: Array[String]): Unit = {
//    getKafkaStream()
  }
}
