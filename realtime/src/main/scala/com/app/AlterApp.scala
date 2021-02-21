package com.app

import java.sql.Date
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.bean.{AlertInfo, EventLog}
import com.util
import com.util.common.KafkaUtil
import contant.MyESUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks.{break, breakable}

/**
  * @author kylinWang
  * @data 2020/3/26 16:29
  *       * {"logType":"event","area":"beijing","uid":"285","eventId":"addCart","itemId":42,
  *       * "os":"android","nextPageId":22,"appId":"gmall0508","mid":"mid_261","pageId":25,"ts":1570689866703}
  **/

object AlterApp{
    //预警分析
    def main(args: Array[String]): Unit = {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val timeFormat = new SimpleDateFormat("HH:mm")

        //建立流式环境
        val conf = new SparkConf().setMaster("local[*]").setAppName("test")
        val ssc = new StreamingContext(conf,Seconds(5))
        //从kafka 获取数据  event0508
        val sourceDStream: InputDStream[(String, String)] = KafkaUtil.getKafkaStream(ssc, "event0508" )//ConstanUtil.EVENT_TOPIC)

        //fixme: 流式数据： DStream, 计算 5min 内的结果
        val eventLogDStream  = sourceDStream.window(Seconds(5*60),Seconds(5))  //fixme: 用微批方式, 加载数据
          .map{
              case (_,jsonValue) =>{
                  //接卸json 获得 每一日志   fixme:讲 json 转换为 样例类
                  val eventlog = JSON.parseObject(jsonValue,classOf[EventLog])

                  //对时间字段进行处理
                  val date = new Date(eventlog.ts)
                  eventlog.logDate = dateFormat.format(date)
                  eventlog.logHour = timeFormat.format(date)
                  //获得一个mid , log 的数据
                  (eventlog.mid, eventlog)
              }
          }

        //2. 获取流后对设备进行分组，数据加工
        val alerInfoDStream = eventLogDStream
          .groupByKey()
          .map{
              case(mid,eventLogIt) =>{
                  val uidSet = new java.util.HashSet[String]()//记录领优惠券的用户
                  val itemSet = new java.util.HashSet[String]()//优惠券商品
                  val eventSet = new java.util.HashSet[String]()//用户操作

                  //定义一个 flag
                  var isClickItem  =  false

                  //导入这个包：import scala.com.util.control.Breaks._
                  breakable{
                      eventLogIt.foreach(eventlog => {
                          eventSet.add(eventlog.eventId)  //事件id
                          if(eventlog.eventId == "coupon"){
                              uidSet.add(eventlog.uid)
                              itemSet.add(eventlog.itemId)
                          } else if(eventlog.eventId == "clickItem"){
                              isClickItem = true
                              break //并结束
                          }
                      })
                  }
                  //返回一个元组, 最终结果获取
                  (!isClickItem && uidSet.size() > 3 , AlertInfo(mid, uidSet, itemSet, eventSet, System.currentTimeMillis()))
              }
          }

        //获取满足true 的数据
        val resultAlertInfoDStream = alerInfoDStream.filter(_._1).map(_._2)

        //数据写入
        resultAlertInfoDStream.foreachRDD(rdd => {
            val t = rdd  //获取的是 rdd 的样例类
            rdd.foreachPartition(alertInfoIt => {
                // 为了实现一分钟只产生一条预警信息, 把mid和分钟组成一个一个最终的id
                MyESUtil.insertBulk("gmall0508_coupon_alert",
                    alertInfoIt.map(info=> (info.mid + "_" + info.ts / 1000 / 60, info)))  // Iterator[Any] -> Iterator[AlterInfo]
            })
        })


        ssc.start()
        ssc.awaitTermination()
    }
}
