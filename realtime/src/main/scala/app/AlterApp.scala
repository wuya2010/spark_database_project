package app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.control.Breaks._
/**
  * @author kylinWang
  * @data 2020/3/26 16:29
  *       * {"logType":"event","area":"beijing","uid":"285","eventId":"addCart","itemId":42,
  *       * "os":"android","nextPageId":22,"appId":"gmall0508","mid":"mid_261","pageId":25,"ts":1570689866703}
  **/
  */

object AlterApp {
    //预警分析
    def main(args: Array[String]): Unit = {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val timeFormat = new SimpleDateFormat("HH:mm")

        //建立流式环境
        val conf = new SparkConf().setMaster("local[*]").setAppName("test")
        val ssc = new StreamingContext(conf,Seconds(5))
        //从kafka 获取数据  event0508
        val sourceDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, ConstanUtil.EVENT_TOPIC)
        val eventLogDStream  = sourceDStream.window(Seconds(5*60),Seconds(5))
          .map{
              case (_,jsonValue) =>{
                  //接卸json 获得 每一日志
                  val eventlog = JSON.parseObject(jsonValue,classOf[EventLog])
                  val date = new Date(eventlog.ts)
                  eventlog.logDate = dateFormat.format(date)
                  eventlog.logHour = timeFormat.format(date)
                  //获得一个mid , log 的数据
                  (eventlog.mid, eventlog)
              }
          }

        //获取流后对设备进行分组
        val alerInfoDStream = eventLogDStream
          .groupByKey()
          .map{
              case(mid,eventLogIt) =>{
                  val uidSet = new util.HashSet[String]()//记录领优惠券的用户
                  val itemSet = new util.HashSet[String]()//优惠券商品
                  val eventSet = new util.HashSet[String]()//用户操作

                  var isClickItem  =  false
                  //导入这个包：import scala.util.control.Breaks._
                  breakable{
                      eventLogIt.foreach(eventlog => {
                          eventSet.add(eventlog.eventId)
                          if(eventlog.eventId == "coupon"){
                              uidSet.add(eventlog.uid)
                              itemSet.add(eventlog.itemId)
                          } else if(eventlog.eventId == "clickItem"){
                              isClickItem = true
                              break //并结束
                          }
                      })
                  }
                  //返回值
                  (!isClickItem && uidSet.size() > 3 , AlertInfo(mid, uidSet, itemSet, eventSet, System.currentTimeMillis()))
              }
          }

        //获取满足true 的数据
        val resultAlertInfoDStream = alerInfoDStream.filter(_._1).map(_._2)





    }
}
