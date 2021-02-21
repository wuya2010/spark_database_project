package com.util.common

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * @author kylinWang
  * @data 2020/3/26 15:53
  *
  */
object KafkaUtil {

  //返回值 string, string
    def getKafkaStream(ssc:StreamingContext,topic:String)={
      val params:Map[String,String] = Map(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getProperties("config.properties", "kafka.broker.list"),
        ConsumerConfig.GROUP_ID_CONFIG -> PropertiesUtil.getProperties("config.properties", "kafka.group")
      )

      //fixme: 定义返回值类型
      KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](
        ssc,params,Set(topic)
      )
    }

  def main(args: Array[String]): Unit = {
//    getKafkaStream()
  }
}
