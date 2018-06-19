package Data_Lake

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkSQL {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application") //定義Spark基本參數
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .getOrCreate()


    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://120.109.150.175:3306/power").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "ammeter").option("user", "hpc").option("password", "hpcverygood").load()
    jdbcDF.createOrReplaceTempView("ammeter_test") //DataFrame來源為藉由JDBC取得MySQL表

    val sqlQuery = "SELECT `pid` FROM `ammeter_test` WHERE `position` not like 'Main'"

    val test = spark.sql(sqlQuery)
    test.show()
    val test_array = test.select("pid").rdd.map(r => r(0)).map(_.toString).collect()
    test_array.foreach(println)

  }


}
