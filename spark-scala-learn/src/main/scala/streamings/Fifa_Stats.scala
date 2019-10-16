package streamings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Fifa_Stats {

  case class Stats(Name : Option[String], Nationality : Option[String], National_Position: Option[String], National_Kit: Option[Double], Club: Option[String], Club_Position: Option[String], Club_Kit: Option[String], Club_Joining : Option[String],
  Contract_Expiry : Option[String], Rating : Option[Double], Height : Option[String], Weight : Option[String], Preffered_Foot : Option[String], Birth_Date : Option[String], Age : Option[String], Preffered_Position : Option[String],
  Work_Rate : Option[String], Weak_foot : Option[String], Skill_Moves : Option[String], Ball_Control : Option[String], Dribbling : Option[Double], Marking : Option[Double], Sliding_Tackle : Option[Double], Standing_Tackle : Option[Double],
  Aggression : Option[Double], Reactions : Option[Double], Attacking_Position : Option[Double], Interceptions : Option[Double], Vision : Option[Double], Composure : Option[Double], Crossing  : Option[Double], Short_Pass : Option[Double],
  Long_Pass : Option[Double], Acceleration : Option[Double], Speed : Option[Double], Stamina : Option[Double], Strength : Option[Double], Balance : Option[Double], Agility : Option[Double], Jumping : Option[Double], Heading : Option[Double],
  Shot_Power : Option[Double], Finishing : Option[Double], Long_Shots : Option[Double], Curve : Option[Double], Freekick_Accuracy : Option[Double], Penalties : Option[Double], Volleys : Option[Double], GK_Positioning : Option[Double],
  GK_Diving : Option[Double], GK_Kicking : Option[Double])

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)


    val config = new SparkConf().setAppName("OnlineRetail").setMaster("local[*]").set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(config)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    import spark.implicits._

    val userSchema = new StructType().add("Name", "string").add("Nationality", "string").add("National_Position", "string").add("National_Kit", "double").add("Club", "string").add("Club_Position", "string")
      .add("Club_Kit", "string").add("Club_Joining", "string").add("Contract_Expiry", "string").add("Rating", "double").add("Height", "string").add("Weight", "string").add("Preffered_Foot", "string").add("Birth_Date", "string")
      .add("Age", "integer").add("Preffered_Position", "string").add("Work_Rate", "string").add("Weak_foot", "string").add("Skill_Moves", "string").add("Ball_Control", "string").add("Dribbling", "double").add("Marking", "double")
      .add("Sliding_Tackle", "double").add("Standing_Tackle", "double").add("Aggression", "double").add("Reactions", "double").add("Attacking_Position", "double").add("Interceptions", "double").add("Vision", "double").add("Composure", "double")
      .add("Crossing", "double").add("Short_Pass", "double").add("Long_Pass", "double").add("Acceleration", "double").add("Speed", "double").add("Stamina", "double").add("Strength", "double").add("Balance", "double")
      .add("Agility", "double").add("Jumping", "double").add("Heading", "double").add("Shot_Power", "double").add("Finishing", "double").add("Long_Shots", "double").add("Curve", "double").add("Freekick_Accuracy", "double")
      .add("Penalties", "double").add("Volleys", "double").add("GK_Positioning", "double").add("GK_Diving", "double").add("GK_Kicking", "double")

    val input = spark.readStream.option("maxFilesPerTrigger","1").schema(userSchema).csv("D:\\spark\\fifa\\fulldata")
    val ds : Dataset[Stats] = input.as[Stats]
    println(input.isStreaming)
    input.printSchema


    // 1. Players with rating grater than or equal to 90 and Preffered_Foot = right
    val query1 = input.filter(input("Rating") >= 90 && input("Preffered_Foot") === "Right").select("Name","Nationality","Club")
    val query1_out = query1
      .writeStream
      .format("console")
      .start()

    // 2. Players with rating grater than or equal to 90 and Ball_Control greater than or equal to 90 using joins.
    val clubs = input.withColumnRenamed("Club", "club_name").select($"club_name").distinct()
    val club_out = clubs
      .writeStream
      .format("console")
      .start()

    val query2 = input.filter(input("Rating") >= 90 && input("Ball_Control") >= 90).join(clubs,$"Club" === $"club_name")
    val query2_out = query2
      .writeStream
      .format("console")
      .start()


    // 3. Players who are good at penalties and freekicks excluding Goalkeepers
    val penalty_players = ds.filter($"Preffered_Position" =!= "GK" && $"Penalties" >= 70).map(data => (data.Name,data.Age,data.Nationality,data.Club,data.Club_Position,data.Penalties)).toDF("Name","Age","Nationality","Club","Club_Position","Penalties")
    val penalty_players_out = penalty_players
      .writeStream
      .format("console")
      .start()

    val freekick_players = ds.filter($"Preffered_Position" =!= "GK" && $"Freekick_Accuracy" >=70).map(data => (data.Name,data.Club,data.Rating,data.Freekick_Accuracy)).toDF("Player_Name","Club","Rating","Freekick_Accuracy")
    val freekick_players_out = freekick_players
      .writeStream
      .format("console")
      .start()

    val freekickPenaltyPlayers = penalty_players.join(freekick_players,$"Name" === $"Player_Name").drop("Player_Name")
    val freekickPenaltyPlayers_out = freekickPenaltyPlayers
      .writeStream
      .format("console")
      .start()

    query1_out.awaitTermination()
    club_out.awaitTermination()
    query2_out.awaitTermination()
    penalty_players_out.awaitTermination()
    freekick_players_out.awaitTermination()
    freekickPenaltyPlayers_out.awaitTermination()
  }

}
