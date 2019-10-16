package streamings

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成有状态统计
  */
object ForeachRDD {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("ForeachRDD").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 3000)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()
    //序列化  rdd -> partition -> record

    result.foreachRDD(rdd => {
      rdd.foreachPartition(partionOfRecords => {
        //val connection = createConnection()
          partionOfRecords.foreach(record => {
            print(record)
            val sql = "insert into wordcount(word,wordcount) values('" + record._1 + "'," + record._2 + ")"
            //connection.createStatement().execute(sql)
          })
          //connection.close()
      })
    })
      //      val connection = createConnection()
      //      rdd.foreach { record =>
      //        val sql = "insert into table wordcount(word,wordcount) values('"+record._1+"',"+record._2+")"
      //        connection.createStatement().execute(sql)
      //      }
      //    })


      ssc.start()
      ssc.awaitTermination()
    }

    def createConnection() = {
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark", "root", "")
    }

    /**
      * 把当前的数据去更新已有的或者是老的数据
      *
      * @param currentValues 当前的
      * @param preValues     老的
      * @return
      */
    def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
      val current = currentValues.sum
      val pre = preValues.getOrElse(0)

      Some(current + pre)
    }
  }
