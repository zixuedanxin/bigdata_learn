package dataframe

import dataframe.Constants.spark
import org.apache.spark.sql.functions._
import spark.implicits._

object JoinOperation extends App {

  val person = Seq(
    (0, "Mahesh Chand",   0 , Seq(100)),
    (1, "Shivangi Gupta", 1 , Seq(500, 250, 100)),
    (2, "Vinod Kandpal",  1 , Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")

  person.show()

  val graduatePerson = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (1, "Ph.D", "EECS", "UC Berkeley"),
    (2, "Ph.D", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "university")

  graduatePerson.show(false)

  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")

  sparkStatus.show()

  //Inner join. Returns only matching columns
  val joinExpression = person("graduate_program") === graduatePerson("id")
  person.join(graduatePerson, joinExpression).show(false)

  //Outer joins
  var joinType = "outer"
  person.join(graduatePerson, joinExpression, joinType).show(false)

  //left outer  join
  person.join(graduatePerson, joinExpression, "left_outer").show(false)

  //right outer  join
  person.join(graduatePerson, joinExpression, "right_outer").show(false)

  //left semi joins
  graduatePerson.join(person, joinExpression, "left_semi").show(false)

  //left anti joins
  graduatePerson.join(person, joinExpression, "left_anti").show(false)

  //cross join
  graduatePerson.join(person, joinExpression, "cross").show(false)

  //complex type joins
  person.withColumnRenamed("id", "personId")
    .join(sparkStatus, expr("array_contains(spark_status, id)")).show(false)


  val gradProgramDup = graduatePerson.withColumnRenamed("id", "graduate_program")
  // run time error same key
  //grandProgramDup.join(person, person("graduate_program") === graduatePerson("graduate_program,"),"outer").show()

  /*
    Handling duplicate columns
    1) Different Join Expression
    2) Dropping the column after the join
    3) Renaming a column before the join
   */

  //solution 1
  person.join(gradProgramDup, "graduate_program").show()

  //solution 2
  person.join(graduatePerson, person.col("graduate_program") === graduatePerson.col("id"))
    .drop(person.col("graduate_program")).show(false)

  //solution 3
  val joinExpr = person.col("graduate_program") === gradProgramDup.col("graduate_program")
  person.join(gradProgramDup, joinExpr).show(false)
}
