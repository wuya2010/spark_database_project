package scala.util

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * @author kylinWang
  * @data 2020/7/6 7:31
  *
  */
object JsonMock {

  val startupNum = 100 // 生成的启动日志的记录数
  val eventNum = 200 // 生成的事件日志的记录数

  // 操作系统的分布
  val osOpts = RandomOptions(("ios", 3), ("android", 7))

  // 日志开始时间
  var startDate: Date = _
  // 日志结束时间
  var endDate: Date = _

  // 地理位置分布
  val areaOpts = RandomOptions(
    ("beijing", 20), ("shanghai", 20), ("guangdong", 20),
    ("hebei", 5), ("heilongjiang", 5), ("shandong", 5),
    ("tianjin", 5), ("guizhou", 5), ("shangxi", 5),
    ("sichuan", 5), ("xinjiang", 5)
  )

  // appId
  val appId = "gmall"

  // com.app 的版本分布
  val versionOpts = RandomOptions(
    ("1.2.0", 50), ("1.1.2", 15),
    ("1.1.3", 30), ("1.1.1", 5))

  // 用户行为的分布(事件分布)
  val eventOpts = RandomOptions(
    ("addFavor", 10), ("addComment", 30),
    ("addCart", 20), ("clickItem", 40))

  // com.app 分发渠道分布
  val channelOpts = RandomOptions(
    ("xiaomi", 10), ("huawei", 20), ("wandoujia", 30),
    ("360", 20), ("tencent", 20), ("baidu", 10), ("website", 10))

  // 生成模拟数据的时候是否结束退出   ==>确定是否退出
  val quitOpts = RandomOptions((true, 5), (false, 95))


  // 模拟出来一条启动日志
  def initOneStartupLog(): String = {
    /*
    `logType` string   COMMENT '日志类型',
    `mid` string COMMENT '设备唯一标识',
    `uid` string COMMENT '用户标识',
    `os` string COMMENT '操作系统', ,
    `appId` string COMMENT '应用id', ,
    `version` string COMMENT '版本号',
    `ts` bigint COMMENT '启动时间',    考虑每个终端的时间的不准群性, 时间是将来在服务器端来生成
    `area` string COMMENT '城市'
    `channel` string COMMENT '渠道'
     */
    val mid: String = "mid_" + RandomNumUtil.randomInt(1, 500)  //怎么生成模拟数据
    val uid: String = "" + RandomNumUtil.randomInt(1, 10000)
    val os: String = osOpts.getRandomOption()  //制造随机值
    val appId: String = this.appId
    val area: String = areaOpts.getRandomOption()
    val version: String = versionOpts.getRandomOption()
    val channel: String = channelOpts.getRandomOption()

    val obj = new JSONObject()

    //生成的json, 字段可以是任意类型
    obj.put("logType", "startup")
    obj.put("mid", mid)
    obj.put("uid", uid)
    obj.put("os", os)
    obj.put("appId", appId)
    obj.put("area", area)
    obj.put("channel", channel)
    obj.put("version", version)

    // 返回 json 格式字符串
    // {"logType":"startup","area":"shanghai","uid":"5750","os":"ios","appId":"gmall","channel":"360","mid":"mid_260","version":"1.2.0"}
    obj.toJSONString
  }

  // 模拟出来一条事件日志  参数: json 格式的启动日志
  def initOneEventLog(startupLogJson: String) = {
    /*`
    logType` string   COMMENT '日志类型',
    `mid` string COMMENT '设备唯一标识',
    `uid` string COMMENT '用户标识',
    `os` string COMMENT '操作系统',
    `appId` string COMMENT '应用id',
    `area` string COMMENT '地区' ,
    `eventId` string COMMENT '事件id',
    `pageId` string COMMENT '当前页',
    `nextPageId` string COMMENT '跳转页',
    `itemId` string COMMENT '商品编号',
    `ts` bigint COMMENT '时间'
     */
    //fixme: json Api 的理解
    val startupLogObj: JSONObject = JSON.parseObject(startupLogJson)

    //创建json 对象
    val eventLogObj = new JSONObject()

    eventLogObj.put("logType", "event")

    //获取 String 中的具体字段信息
    eventLogObj.put("mid", startupLogObj.getString("mid"))
    eventLogObj.put("uid", startupLogObj.getString("uid"))
    eventLogObj.put("os", startupLogObj.getString("os"))
    eventLogObj.put("appId", this.appId)
    eventLogObj.put("area", startupLogObj.getString("area"))
    eventLogObj.put("eventId", eventOpts.getRandomOption())
    eventLogObj.put("pageId", RandomNumUtil.randomInt(1, 50))
    eventLogObj.put("nextPageId", RandomNumUtil.randomInt(1, 50))
    eventLogObj.put("itemId", RandomNumUtil.randomInt(1, 50))

    eventLogObj.toJSONString
  }

  //fixme: 整体调用-- 开始生成日志
  def generateLog(): Unit = {

    (0 to startupNum).foreach(_ => {
      // 生成一条启动日志
      val oneStartupLog: String = initOneStartupLog()

      // 发送启动日志
      LogUploader.sendLog(oneStartupLog) // 怎么发送日志

      // 模拟出来多条事件日志
      while (!quitOpts.getRandomOption()) {  //true / false
        // 生成一条事件日志
        val oneEventLog: String = initOneEventLog(oneStartupLog)  //fixme : 根据 启动日志生成 事件日志
        // 发送事件日志
        LogUploader.sendLog(oneEventLog)

        Thread.sleep(100)
      }

      Thread.sleep(1000)
    })
  }

  def main(args: Array[String]): Unit = {
//    // 测试
//    println(initOneEventLog(" {\"logType\":\"startup\",\"area\":\"shanghai\",\"uid\":\"5750\",\"os\":\"ios\",\"appId\":\"gmall\",\"channel\":\"360\",\"mid\":\"mid_260\",\"version\":\"1.2.0\"}"))
//    // 格式
////    "{\"key\":\"d\"}"
//    val t =  0 to startupNum
//
//    t.foreach(println)  //打印输出 0- 100

    generateLog


    // 生成数据
//    {"logType":"startup","area":"shandong","uid":"8608","os":"android","appId":"gmall","channel":"tencent","mid":"mid_164","version":"1.2.0"}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":10,"os":"android","nextPageId":29,"appId":"gmall","mid":"mid_164","pageId":3}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addFavor","itemId":32,"os":"android","nextPageId":21,"appId":"gmall","mid":"mid_164","pageId":1}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":16,"os":"android","nextPageId":25,"appId":"gmall","mid":"mid_164","pageId":24}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":31,"os":"android","nextPageId":9,"appId":"gmall","mid":"mid_164","pageId":33}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"clickItem","itemId":31,"os":"android","nextPageId":20,"appId":"gmall","mid":"mid_164","pageId":50}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":22,"os":"android","nextPageId":44,"appId":"gmall","mid":"mid_164","pageId":11}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":27,"os":"android","nextPageId":34,"appId":"gmall","mid":"mid_164","pageId":12}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"clickItem","itemId":13,"os":"android","nextPageId":34,"appId":"gmall","mid":"mid_164","pageId":9}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"clickItem","itemId":29,"os":"android","nextPageId":33,"appId":"gmall","mid":"mid_164","pageId":43}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addCart","itemId":47,"os":"android","nextPageId":5,"appId":"gmall","mid":"mid_164","pageId":23}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addCart","itemId":23,"os":"android","nextPageId":46,"appId":"gmall","mid":"mid_164","pageId":5}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addCart","itemId":17,"os":"android","nextPageId":49,"appId":"gmall","mid":"mid_164","pageId":16}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"clickItem","itemId":30,"os":"android","nextPageId":21,"appId":"gmall","mid":"mid_164","pageId":14}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":10,"os":"android","nextPageId":30,"appId":"gmall","mid":"mid_164","pageId":34}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":45,"os":"android","nextPageId":12,"appId":"gmall","mid":"mid_164","pageId":9}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":9,"os":"android","nextPageId":17,"appId":"gmall","mid":"mid_164","pageId":47}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"clickItem","itemId":9,"os":"android","nextPageId":12,"appId":"gmall","mid":"mid_164","pageId":9}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":42,"os":"android","nextPageId":40,"appId":"gmall","mid":"mid_164","pageId":5}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"clickItem","itemId":4,"os":"android","nextPageId":5,"appId":"gmall","mid":"mid_164","pageId":50}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"clickItem","itemId":18,"os":"android","nextPageId":34,"appId":"gmall","mid":"mid_164","pageId":48}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"clickItem","itemId":14,"os":"android","nextPageId":18,"appId":"gmall","mid":"mid_164","pageId":41}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"clickItem","itemId":44,"os":"android","nextPageId":18,"appId":"gmall","mid":"mid_164","pageId":2}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addCart","itemId":24,"os":"android","nextPageId":34,"appId":"gmall","mid":"mid_164","pageId":19}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addFavor","itemId":3,"os":"android","nextPageId":44,"appId":"gmall","mid":"mid_164","pageId":17}
//    {"logType":"event","area":"shandong","uid":"8608","eventId":"addComment","itemId":37,"os":"android","nextPageId":15,"appId":"gmall","mid":"mid_164","pageId":41}

  }
}
