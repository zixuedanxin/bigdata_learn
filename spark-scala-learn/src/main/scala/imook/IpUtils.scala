package imook

// import com.ggstar.util.ip.IpHelper

/**
  * ip解析工具类  把ip转化为城市
  */
object IpUtils {

  def getCity(ip: String) = {

    // IpHelper.findRegionByIp(ip)
    ip
  }

  def main(args: Array[String]): Unit = {

    println(getCity("58.30.15.255"))

  }

}
