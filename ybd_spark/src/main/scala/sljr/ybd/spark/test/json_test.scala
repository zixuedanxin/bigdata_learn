package sljr.ybd.spark.test
import org.json4s._
import scala.tools.reflect.ToolBox
import com.alibaba.fastjson.JSON
import org.json4s.jackson.JsonMethods._
object  json_test{
  def main(args: Array[String]): Unit = {
    var myString="""{"database":"cm","table":"CM_VERSION","type":"update","ts":1511421401,"xid":63829,"commit":true,"data":{"VERSION":"5.11.2","GUID":"7d5767f2-405d-418f-889a-f9cdcd05d6f9","LAST_UPDATE_INSTANT":1507619792924,"TS":1509368798796,"HOSTNAME":"cdh1.com/192.168.188.80","LAST_ACTIVE_TIMESTAMP":1511421401508},"old":{"LAST_ACTIVE_TIMESTAMP":1511421386506}}
              """
    /*val json = parse(
      """{"database":"cm","table":"CM_VERSION","type":"update","ts":1511421401,"xid":63829,"commit":true,"data":{"VERSION":"5.11.2","GUID":"7d5767f2-405d-418f-889a-f9cdcd05d6f9","LAST_UPDATE_INSTANT":1507619792924,"TS":1509368798796,"HOSTNAME":"cdh1.com/192.168.188.80","LAST_ACTIVE_TIMESTAMP":1511421401508},"old":{"LAST_ACTIVE_TIMESTAMP":1511421386506}}
        """)*/
    //println(json)
    //
    val tb = scala.reflect.runtime.currentMirror.mkToolBox()
    //var ms=(json\"data")//.values.toString
    val json=JSON.parseObject(myString)
    val fet=json.get("database")
    println(fet)
    println(json.getJSONObject("data"))
    val jsonKey=json.getJSONObject("data").keySet()//.forEach(x=>print(x))
    val iter = jsonKey.iterator
    while (iter.hasNext) {
      val instance = iter.next()
      val value = json.getJSONObject("data").get(instance).toString
      println("key: " + instance + " value:" + value)
    }

    println(jsonKey)
    //println(ms)
    /*var tps=ms.substring(1, ms.length - 1)
      .split(",")
      .map(_.split(":"))
      .map{case Array(k, v) => (k->v)}
      .toMap
    println(tps)*/
    //print(tb.parse(ms))


  }
}