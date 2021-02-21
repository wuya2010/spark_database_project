package com

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.bean.StartupLog

/**
  * @author kylinWang
  * @data 2020/3/26 16:29
  *
  */
object DauApp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DauApp").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sourceDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, ConstantUtil.STARTUP_TOPIC)
    // 1. 封装数据 , 返回的是 log
    val startupLogDStream = sourceDStream.map {
      case (_, value) =>
        val log = JSON.parseObject(value, classOf[StartupLog])
        log.logDate = new SimpleDateFormat("yyyy-MM-dd").format(log.ts)
        log.logHour = new SimpleDateFormat("HH").format(log.ts)
        log
    }

    //redis 进行去重 ， hbase 与 其他
    // transform 与 map 的区别
    // transform: Return a new DStream in which each RDD is generated by applying a function
    //   * on each RDD of 'this' DStream.
    var filteredDStream = startupLogDStream.transform(rdd =>{
      val client = RedisUtil.getJedisClient
      //Return all the members (elements) of the set value stored at key
      val uidSet = client.smembers("startup0508"+ new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      //将变量广播出去
      val uidSetBC = ssc.sparkContext.broadcast(uidSet)
      //过滤已经启动过的
      rdd.filter(log => {
        !uidSetBC.value.contains(log.uid)}
      )
    })

    filteredDStream = filteredDStream.map(log =>(log.uid,log))
      .groupByKey()
      .map{
        case (_, logIt) => logIt.toList.sortBy(_.ts).head
    }



  }
}