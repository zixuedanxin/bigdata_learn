package org.roy.demo.streaming.state

import java.sql.Timestamp

object TmpMain {

  case class orderInfoStoretest(orderId: String, otype: Int, storeId: String, money: Double, orderDate: String)

  def main(args: Array[String]): Unit = {

    val s = orderInfoStoretest("1", 1, "200", 100.0, "2019")
    val events: Iterator[orderInfoStoretest] = Iterator(orderInfoStoretest("2", 1, "200", 100.0, "2019"), orderInfoStoretest("1", 1, "200", 100.0, "2019"))
    val seqx = events.toSeq
    println(seqx)
    val order_moneys = seqx.map(_.money).reduce(_ + _)
    println(order_moneys)
    val it = Iterator("Baidu", "Google", "Runoob", "Taobao")

    //其实只会一个
    var moen = 0.0

    val m = seqx.foreach(e => {
      println("ffff" + e.money)
      moen += e.money
    })
    println(moen)

  }
}
