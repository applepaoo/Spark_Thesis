package Data_Lake


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}





object WritePowerDataMinuteToHBase {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .getOrCreate()

    val ssc = new StreamingContext(sc, Seconds(60)) //建立Streaming Context 接口
    val topicMap = "PowerData_Minute_HBase".split(":").map((_, 1)).toMap  //指定kafka topic name
    val zkQuorum = "140.128.98.31:2181"; //ZooKeeper 位置
    val group = "test-consumer-group"
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) //接kafka串流，形成DStream (Spark Streaming專用格式)
    val lines_split = lines.map(x => x.split(",")).map(x => {(x(0), x(1), x(2), x(3))}) //時間,地點,千瓦,總千瓦小時

/*    lines_split.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {//循環分區
      val conf = HBaseConfiguration.create();
        //val conf = new HBaseConfiguration
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "140.128.98.31")
        val connection = ConnectionFactory.createConnection(conf); //獲取HBase連接,分區創建一個連接，分區不跨節點，不需要序列化
        partitionRecords.foreach(s => {
          val table = connection.getTable(TableName.valueOf("powerdata_minute_hbase")) //HBase TableName
          val put = new Put(Bytes.toBytes(s._2.toString + "_" + s._1.toString.substring(0, 16))) //ROW KEY 只取2018-03-07 17:38(舉例用)
          put.addColumn(Bytes.toBytes("powerdata_minute"), Bytes.toBytes("time"), Bytes.toBytes(s._1.toString)) //時間
          put.addColumn(Bytes.toBytes("powerdata_minute"), Bytes.toBytes("location"), Bytes.toBytes(s._2.toString)) //地點
          put.addColumn(Bytes.toBytes("powerdata_minute"), Bytes.toBytes("kw"), Bytes.toBytes(s._3.toString)) //千瓦
          put.addColumn(Bytes.toBytes("powerdata_minute"), Bytes.toBytes("totalkwh"), Bytes.toBytes(s._4.toString)) //總千瓦小時
          table.put(put)//將數據寫入HBase，若出錯關閉table
          table.close()//分區數據寫入HBase后關閉連接
          println(s._1.toString + ","+ s._2 + "寫入HBase")
        })
      })
    })*/
    ssc.start()
    ssc.awaitTermination()
  }
}
