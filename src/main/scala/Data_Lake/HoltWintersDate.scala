package Data_Lake

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.cloudera.sparkts.models.HoltWinters
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


object HoltWintersDate {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Simple Application") //定義Spark基本參數
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .getOrCreate()


    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://120.109.150.175:3306/power").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "PowerHour").option("user", "hpc").option("password", "hpcverygood").load()
    jdbcDF.createOrReplaceTempView("PowerHOur_test") //DataFrame來源為藉由JDBC取得MySQL表


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

    def trainAndPredict(a: String) = {
      //取得MySQL資料並利用HoltWinters預測未來兩天
      var test = spark.sql(a)
      println("querty:" + a)
      println("訓練集")
      test.show(48)
      val dataTrain = test.select("x").rdd.map(r => r(0)).map(_.toString).map(_.toDouble).collect()
      val ts = Vectors.dense(dataTrain)
      val hModel = HoltWinters.fitModel(ts, 24, "Additive", "BOBYQA")
      //Multiplicative, Additive
      val forecast = hModel.forecast(ts, ts)
      val forecastArray = forecast.toArray
      println("開始預測")
      forecastArray.foreach(println)
      println("共預測了" + forecastArray.length + "筆資料")

      val writeToMySQLArray = Array.ofDim[String](24, 4) //宣告存放預測資料的二維陣列

      for (i <- 0 to 23) {
        //填入預測資料至二維陣列

        writeToMySQLArray(i)(0) = getNowDate() //時間
        writeToMySQLArray(i)(1) = i.toString //小時
        writeToMySQLArray(i)(2) = "LIB-4" //地點
        writeToMySQLArray(i)(3) = forecastArray(i).toString //預測電力度數P值

      }

      //寫入至MySQL
      println("寫入開始...")
      val predictRDD = spark.sparkContext.parallelize(writeToMySQLArray)
      //創建RDD
      val schema = StructType(List(StructField("date", StringType, true), StructField("hr", IntegerType, true), StructField("Meter_id", StringType, true), StructField("P", DoubleType, true)))
      //定義schema
      val rowRDD = predictRDD.map(p => Row(p(0).toString, p(1).toInt, p(2).toString, p(3).toDouble))
      //RDD指定元素型態
      val predictDF = spark.createDataFrame(rowRDD, schema)
      //
      //設置寫入MySQL相關變量
      val prop = new Properties()
      prop.put("user", "hpc")
      prop.put("password", "hpcverygood")
      prop.put("driver", "com.mysql.jdbc.Driver")
      predictDF.write.mode("append").jdbc("jdbc:mysql://120.109.150.175:3306/power", "power.PowerHourPredict", prop) //寫入
      println("已寫入至MySQL")

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

    //每天相對應到符合的條件
    day match {
      case 1 =>
        var sqlDate = "'" + sdf.format(cal.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_6.getTime) + "'"
        var sqlQuery = "select round(`p`/1000, 1) as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate + ", " + sqlDate_1 + ")"
        trainAndPredict(sqlQuery)

      case 2 =>
        var sqlDate = "'" + sdf.format(cal.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_8.getTime) + "'"
        var sqlQuery = "select round(`p`/1000, 1) as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")"

      case 3 =>
        var sqlDate = "'" + sdf.format(cal_1.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_2.getTime) + "'"
        var sqlQuery = "select round(`p`/1000, 1) as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")"
        trainAndPredict(sqlQuery)

      case 4 =>
        var sqlDate = "'" + sdf.format(cal_1.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_2.getTime) + "'"
        var sqlQuery = "select round(`p`/1000, 1) as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")"
        trainAndPredict(sqlQuery)

      case 5 =>
        var sqlDate = "'" + sdf.format(cal_1.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_2.getTime) + "'"
        var sqlQuery = "select round(`p`/1000, 1) as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")"
        trainAndPredict(sqlQuery)

      case 6 =>
        var sqlDate = "'" + sdf.format(cal.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_6.getTime) + "'"
        var sqlQuery = "select round(`p`/1000, 1) as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate + ", " + sqlDate_1 + ")"
        trainAndPredict(sqlQuery)

      case 7 =>
        var sqlDate = "'" + sdf.format(cal.getTime) + "'"
        var sqlDate_1 = "'" + sdf.format(cal_8.getTime) + "'"
        var sqlQuery = "select round(`p`/1000, 1) as x from PowerHour_test where `Meter_id` = 'LIB-4' and `p`/1000 > 10 and `date` in " + "(" + sqlDate_1 + ", " + sqlDate + ")"
        trainAndPredict(sqlQuery)
    }

  }
}
