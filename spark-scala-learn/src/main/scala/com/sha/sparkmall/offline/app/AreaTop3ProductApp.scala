package com.sha.sparkmall.offline.app

import com.sha.sparkmall.common.util.PropertiesUtil
import com.sha.sparkmall.offline.udf.CityRemarkUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author shamin
  * @create 2019-04-10 14:28 
  */
object AreaTop3ProductApp {
  def main(args: Array[String]): Unit = {
    //需求三：求各区域点击量 top3
    //       地区	 商品名称	点击次数  	城市备注
    //结果集：华北	 商品A	    100000	  北京21.2%，天津13.2%，其他65.6%
    val sparkConf = new SparkConf().setAppName("AreaTop3ProductApp").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val properties = PropertiesUtil.load("config.properties")
    val database = properties.getProperty("hive.database")
    sparkSession.sql("use " + database)

    //注册函数
    val cityRemarkUDAF = new CityRemarkUDAF
    sparkSession.udf.register("cityRemark",cityRemarkUDAF)
    //用sparkSql实现
    //首先将user_visit_action 和 city_info表进行join
    sparkSession.sql("select  ci.area,uv.click_product_id,ci.city_name from user_visit_action uv join city_info ci on uv.city_id = ci.city_id where uv.click_product_id >0 ").createOrReplaceTempView("area_product_detail")
    //按地区和商品进行分组，求点击的次数
    sparkSession.sql("select area,click_product_id,count(*) clickcount,cityRemark(city_name) cityRatio from area_product_detail group by area,click_product_id").createOrReplaceTempView("area_product_count")
    //用开窗函数每个地区的商品点击的排名
    sparkSession.sql("select area,click_product_id,clickcount,cityRatio,row_number() over(partition by area order by clickcount desc) rn from area_product_count") .createOrReplaceTempView("area_product_sorted")
   //取每个地区的商品点击的排名前3
    sparkSession.sql("select area,click_product_id,clickcount,cityRatio from area_product_sorted where rn <= 3").show(100)

  }
}
