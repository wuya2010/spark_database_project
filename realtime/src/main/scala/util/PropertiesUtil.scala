package util

import java.util.Properties

import scala.collection.mutable

/**
  * @author kylinWang
  * @data 2020/3/26 15:53
  *
  */

// 通过传入2个参数获取想要的结果，在代码中进行相应的转换
object PropertiesUtil {
  val map = mutable.Map[String,Properties]()
  //将属性放入map中
  // gerOrElse 与 getOrElseUpdate 的比较
  def getProperties(propFile:String,propName:String) ={
      val props = map.getOrElseUpdate(propFile,{
        val fis = ClassLoader.getSystemResourceAsStream(propFile)
        val props = new Properties
        props.load(fis)
        props   //将属性放回
      })

      val value = props.getProperty(propName)
      map.put(propName, props)
      value
  }

  def main(args: Array[String]): Unit = {
    println(getProperties("E:\\01_myselfProject\\Spark_wangmall\\wangmall_realtime\\src\\main\\resources\\config.properties","redis.port"))
  }
}
