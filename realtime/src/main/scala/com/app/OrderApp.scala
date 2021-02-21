package com.app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.bean.OrderInfo
import com.util.common.KafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author kylinWang
  * @data 2020/7/10 7:33
  * 实现： 从kafka中读取订单数据并写入ES
  */
object OrderApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc = new StreamingContext(conf, Seconds(2))

    //获取流
    val sourceDStream: InputDStream[(String, String)] = KafkaUtil.getKafkaStream(ssc,"topic_order")


    //核心代码
    val orerInfoDStream: DStream[OrderInfo] = sourceDStream.map {
      // 对数据格式做调整
      case (_, value) => {

        //json 样例类
        val orderInfo = JSON.parseObject(value, classOf[OrderInfo])

        //对流数据，进行etl清洗，fixme: 所有的 udf 都是 dataframe 的函数， 提供对rdd的处理
        orderInfo.consignee = orderInfo.consignee.substring(0, 1) + "**" // 李小名 => 李**
        orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) +
          "****" + orderInfo.consignee_tel.substring(7, 11)

        // 计算 createDate 和 createHour
        val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(orderInfo.create_time)  //fixme: 转换为 date 类型
        orderInfo.create_date = new SimpleDateFormat("yyyy-MM-dd").format(date)
        orderInfo.create_hour = new SimpleDateFormat("HH").format(date)
        orderInfo
      }
    }

    //需要导入依赖
    import org.apache.phoenix.spark._

    //2. 把数据写入到 Phoenix ， 写入到 hbase, 数据可查
    orerInfoDStream.foreachRDD( dsStre  => {  // dsStre
      //fixme:
      dsStre.saveToPhoenix(
        "GMALL_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Some("hadoop201,hadoop202,hadoop203:2181"))
    })

    orerInfoDStream.print

    ssc.start()  //怎么停止
    ssc.awaitTermination()


  }
}
