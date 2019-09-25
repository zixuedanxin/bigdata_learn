package cn.huage.beans

import cn.huage.utils.DataUtils

/**
  * @author zhangjin
  * @create 2018-12-02 10:44
  */

class Log(
           val sessionid: String,
           val advertisersid: Int,
           val adorderid: Int,
           val adcreativeid: Int,
           val adplatformproviderid: Int,
           val sdkversion: String,
           val adplatformkey: String,
           val putinmodeltype: Int,
           val requestmode: Int,
           val adprice: Double,
           val adppprice: Double,
           val requestdate: String,
           val ip: String,
           val appid: String,
           val appname: String,
           val uuid: String,
           val device: String,
           val client: Int,
           val osversion: String,
           val density: String,
           val pw: Int,
           val ph: Int,
           val long: String,
           val lat: String,
           val provincename: String,
           val cityname: String,
           val ispid: Int,
           val ispname: String,
           val networkmannerid: Int,
           val networkmannername: String,
           val iseffective: Int,
           val isbilling: Int,
           val adspacetype: Int,
           val adspacetypename: String,
           val devicetype: Int,
           val processnode: Int,
           val apptype: Int,
           val district: String,
           val paymode: Int,
           val isbid: Int,
           val bidprice: Double,
           val winprice: Double,
           val iswin: Int,
           val cur: String,
           val rate: Double,
           val cnywinprice: Double,
           val imei: String,
           val mac: String,
           val idfa: String,
           val openudid: String,
           val androidid: String,
           val rtbprovince: String,
           val rtbcity: String,
           val rtbdistrict: String,
           val rtbstreet: String,
           val storeurl: String,
           val realip: String,
           val isqualityapp: Int,
           val bidfloor: Double,
           val aw: Int,
           val ah: Int,
           val imeimd5: String,
           val macmd5: String,
           val idfamd5: String,
           val openudidmd5: String,
           val androididmd5: String,
           val imeisha1: String,
           val macsha1: String,
           val idfasha1: String,
           val openudidsha1: String,
           val androididsha1: String,
           val uuidunknow: String,
           val userid: String,
           val iptype: Int,
           val initbidprice: Double,
           val adpayment: Double,
           val agentrate: Double,
           val lrate: Double,
           val adxrate: Double,
           val title: String,
           val keywords: String,
           val tagid: String,
           val callbackdate: String,
           val channelid: String,
           val mediatype: Int
         ) extends Product with Serializable {

  // 通过角标的方式来访问类的成员变量
  override def productElement(n: Int): Any = n match {
    case 0 => sessionid
    case 1 => advertisersid
    case 2 => adorderid
    case 3 => adcreativeid
    case 4 => adplatformproviderid
    case 5 => sdkversion
    case 6 => adplatformkey
    case 7 => putinmodeltype
    case 8 => requestmode
    case 9 => adprice
    case 10 => adppprice
    case 11 => requestdate
    case 12 => ip
    case 13 => appid
    case 14 => appname
    case 15 => uuid
    case 16 => device
    case 17 => client
    case 18 => osversion
    case 19 => density
    case 20 => pw
    case 21 => ph
    case 22 => long
    case 23 => lat
    case 24 => provincename
    case 25 => cityname
    case 26 => ispid
    case 27 => ispname
    case 28 => networkmannerid
    case 29 => networkmannername
    case 30 => iseffective
    case 31 => isbilling
    case 32 => adspacetype
    case 33 => adspacetypename
    case 34 => devicetype
    case 35 => processnode
    case 36 => apptype
    case 37 => district
    case 38 => paymode
    case 39 => isbid
    case 40 => bidprice
    case 41 => winprice
    case 42 => iswin
    case 43 => cur
    case 44 => rate
    case 45 => cnywinprice
    case 46 => imei
    case 47 => mac
    case 48 => idfa
    case 49 => openudid
    case 50 => androidid
    case 51 => rtbprovince
    case 52 => rtbcity
    case 53 => rtbdistrict
    case 54 => rtbstreet
    case 55 => storeurl
    case 56 => realip
    case 57 => isqualityapp
    case 58 => bidfloor
    case 59 => aw
    case 60 => ah
    case 61 => imeimd5
    case 62 => macmd5
    case 63 => idfamd5
    case 64 => openudidmd5
    case 65 => androididmd5
    case 66 => imeisha1
    case 67 => macsha1
    case 68 => idfasha1
    case 69 => openudidsha1
    case 70 => androididsha1
    case 71 => uuidunknow
    case 72 => userid
    case 73 => iptype
    case 74 => initbidprice
    case 75 => adpayment
    case 76 => agentrate
    case 77 => lrate
    case 78 => adxrate
    case 79 => title
    case 80 => keywords
    case 81 => tagid
    case 82 => callbackdate
    case 83 => channelid
    case 84 => mediatype
  }

  // 元数   类中有多少属性
  override def productArity: Int = 85

  // 2个对象是否是同一种类型
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Log]
}

object Log {

  import cn.huage.beans.RichString._

  def apply(arr: Array[String]): Log = new Log(
    arr(0),
    DataUtils.parseInt(arr(1)),
    arr(2).parseInt,
    arr(3).parseInt,
    arr(4).parseInt,
    arr(5),
    arr(6),
    arr(7).parseInt,
    arr(8).parseInt,
    arr(9).parseDouble,
    arr(10).parseDouble,
    arr(11),
    arr(12),
    arr(13),
    arr(14),
    arr(15),
    arr(16),
    arr(17).parseInt,
    arr(18),
    arr(19),
    arr(20).parseInt,
    arr(21).parseInt,
    arr(22),
    arr(23),
    arr(24),
    arr(25),
    arr(26).parseInt,
    arr(27),
    arr(28).parseInt,
    arr(29),
    arr(30).parseInt,
    arr(31).parseInt,
    arr(32).parseInt,
    arr(33),
    arr(34).parseInt,
    arr(35).parseInt,
    arr(36).parseInt,
    arr(37),
    arr(38).parseInt,
    arr(39).parseInt,
    arr(40).parseDouble,
    arr(41).parseDouble,
    arr(42).parseInt,
    arr(43),
    arr(44).parseDouble,
    arr(45).parseDouble,
    arr(46),
    arr(47),
    arr(48),
    arr(49),
    arr(50),
    arr(51),
    arr(52),
    arr(53),
    arr(54),
    arr(55),
    arr(56),
    arr(57).parseInt,
    arr(58).parseDouble,
    arr(59).parseInt,
    arr(60).parseInt,
    arr(61),
    arr(62),
    arr(63),
    arr(64),
    arr(65),
    arr(66),
    arr(67),
    arr(68),
    arr(69),
    arr(70),
    arr(71),
    arr(72),
    arr(73).parseInt,
    arr(74).parseDouble,
    arr(75).parseDouble,
    arr(76).parseDouble,
    arr(77).parseDouble,
    arr(78).parseDouble,
    arr(79),
    arr(80),
    arr(81),
    arr(82),
    arr(83),
    arr(84).parseInt
  )

}