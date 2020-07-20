package com.alibaba.canal

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * @author kylinWang
  * @data 2020/7/10 7:25
  *   实现 Kafka 生产者
  */
object MyKafkaSender {
  val props = new Properties()
  // Kafka服务端的主机名和端口号
  props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9093")
  // key序列化
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // value序列化
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)


  //向生产者发送消息
  def sendToKafka(topic: String, content: String) = {

    producer.send(new ProducerRecord[String, String](topic, content))

  }
}
