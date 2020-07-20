package com.alibaba.web_logger.controller

import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.{RequestParam, RestController}

/**
  * @author kylinWang
  * @data 2020/7/6 7:53
  *
  */
class LoggerController {
  //http://localhost:8080/log
  @RestController
  def doLog(@RequestParam("log")log:String)={

    //添加时间戳
    val logWithTS: String = addTS(log)
    //保存到log
    saveLog2File(logWithTS)
    //保存到kafka
    send2Kafka(logWithTS)

  }

  //传递到kafka
  @Autowired
  var templete : KafkaTemplate[String, String] = _
  def send2Kafka(logWithTS: String) = {
    var topic: String = ConstantUtil.STARTUP_TOPIC
    if (JSON.parseObject(logWithTS).getString("logType") == "event") {
      topic = ConstantUtil.EVENT_TOPIC
    }
    templete.send(topic, logWithTS)
  }



  //传递到logger
  private val logger: Logger = LoggerFactory.getLogger(classOf[LoggerController])
  def saveLog2File(logWithTS:String) = {
    logger.info(logWithTS)
  }


  /**
  给日志添加时间戳
    */
  def addTS(log: String) = {
    val jsonObj: JSONObject = JSON.parseObject(log)
    jsonObj.put("ts", System.currentTimeMillis())
    jsonObj.toJSONString
  }


}
