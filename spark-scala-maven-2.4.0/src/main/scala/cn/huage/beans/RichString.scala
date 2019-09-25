package cn.huage.beans

import org.apache.commons.lang3.StringUtils

/**
  * @author zhangjin
  * @create 2018-12-02 10:47
  */
class RichString(val str: String) {

  def parseInt = {
    try {
      if (StringUtils.isNotEmpty(str)) str.toInt else 0
    } catch {
      case _: Exception => 0
    }
  }


  def parseDouble = {
    try {
      if (StringUtils.isNotEmpty(str)) str.toDouble else 0d
    } catch {
      case _: Exception => 0d
    }
  }

}

object RichString {
  implicit def str2RichString(str: String) = new RichString(str)
}
