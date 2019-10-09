package com.xuzh.utils

/**
  * @author zhangjin
  * @create 2018-12-02 10:48
  */

import org.apache.commons.lang3.StringUtils

object DataUtils {


  def parseInt(str: String): Int = {
    try {
      if (StringUtils.isNotEmpty(str)) str.toInt else 0
    } catch {
      case _: Throwable => 0
    }
  }


  def parseDouble(str: String): Double = {
    try {
      if (StringUtils.isNotEmpty(str)) str.toDouble else 0.0
    } catch {
      case _: Throwable => 0.0
    }
  }

}
