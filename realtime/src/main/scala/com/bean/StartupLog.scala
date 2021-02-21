package scala.bean

/**
  * @author kylinWang
  * @data 2020/7/7 7:40
  *
  */
case class StartupLog(
                       mid: String,
                       uid: String,
                       appId: String,
                       area: String,
                       os: String,
                       channel: String,
                       logType: String,
                       version: String,
                       ts: Long,
                       var logDate: String,  //fixme: var 变量是什么？
                       var logHour: String
                     )
