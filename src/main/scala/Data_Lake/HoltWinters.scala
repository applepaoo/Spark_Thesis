package Data_Lake

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object HoltWinters{
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .getOrCreate()


      val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://120.109.150.175:3306/power").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "PowerHour").option("user", "hpc").option("password", "hpcverygood").load()
    jdbcDF.createOrReplaceTempView("PowerHOur_test")


    def getNowDate(): String = {
      //取得現在時間
      var now: Date = new Date()
      var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      var hehe = dateFormat.format(now)
      hehe
    }

    def determineDayOfTheWeek(a: Int): Int = {
      //取得星期幾
      var dayForWeek = 0
      if (a == 1) {
        dayForWeek = 7
        return dayForWeek
      } else {
        dayForWeek = a - 1
        return dayForWeek
      }
    }

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    //規定好日期格式
    val cal = java.util.Calendar.getInstance();
    val cal_1 = java.util.Calendar.getInstance();
    val cal_2 = java.util.Calendar.getInstance();
    val cal_6 = java.util.Calendar.getInstance();
    val cal_8 = java.util.Calendar.getInstance();
    //val date = "2018-05-13"
    cal.setTime(sdf.parse(getNowDate())) //解析日期
    cal.add(java.util.Calendar.DATE, -7) //往前一星期

    cal_1.setTime(sdf.parse(getNowDate())) //解析日期
    cal_1.add(java.util.Calendar.DATE, -1) //往前1天

    cal_2.setTime(sdf.parse(getNowDate())) //解析日期
    cal_2.add(java.util.Calendar.DATE, -2) //往前2天

    cal_6.setTime(sdf.parse(getNowDate())) //解析日期
    cal_6.add(java.util.Calendar.DATE, -6) //往前6天

    cal_8.setTime(sdf.parse(getNowDate())) //解析日期
    cal_8.add(java.util.Calendar.DATE, -8) //往前6天

    val day = determineDayOfTheWeek(cal.get(Calendar.DAY_OF_WEEK)) //找出星期幾
    println("現在日期:" + getNowDate() + " 星期" + day)
    println("上週日期:" + sdf.format(cal.getTime))


    day match {
      case 1 =>
        var sqlDate = "'" + sdf.format(cal.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_6.getTime) + "'"
        var sqlQuery = "select `p`/1000 as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate + ", " + sqlDate_1 + ")" //上週一與二日期當條件
      var test = spark.sql(sqlQuery)
        test.show(48)
        println(sqlQuery)

      case 2 =>
        var sqlDate = "'" + sdf.format(cal.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_8.getTime) + "'"
        var sqlQuery = "select `p`/1000 as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")" //上週一與二日期當條件
      var test = spark.sql(sqlQuery)
        test.show(48)
        println(sqlQuery)

      case 3 =>
        var sqlDate = "'" + sdf.format(cal_1.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_2.getTime) + "'"
        var sqlQuery = "select `p`/1000 as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")"
        var test = spark.sql(sqlQuery)
        test.show(48)
        println(sqlQuery)

      case 4 =>
        var sqlDate = "'" + sdf.format(cal_1.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_2.getTime) + "'"
        var sqlQuery = "select `p`/1000 as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")"
        var test = spark.sql(sqlQuery)
        test.show(48)
        println(sqlQuery)

      case 5 =>
        var sqlDate = "'" + sdf.format(cal_1.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_2.getTime) + "'"
        var sqlQuery = "select `p`/1000 as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")"
        var test = spark.sql(sqlQuery)
        test.show(48)
        println(sqlQuery)

      case 6 =>
        var sqlDate = "'" + sdf.format(cal.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_6.getTime) + "'"
        var sqlQuery = "select `p`/1000 as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate + ", " + sqlDate_1 + ")" //上週一與二日期當條件
      var test = spark.sql(sqlQuery)
        test.show(48)
        println(sqlQuery)

      case 7 =>
        var sqlDate = "'" + sdf.format(cal.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_8.getTime) + "'"
        var sqlQuery = "select `p`/1000 as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")" //上週一與二日期當條件
      var test = spark.sql(sqlQuery)
        test.show(48)
        println(sqlQuery)
    }

  }
}
