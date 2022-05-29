package spark_tuning

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.alibaba.fastjson.JSONObject

object SaveSellCourse {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "lsh")
    val sparkConf = new SparkConf().setAppName("test")
      .set("spark.sql.hive.convertMetastoreParque", "false")
      .set("spark.sql.parquet.writeLegacyFormat", "true")
      .set("spark.sql.storeAssignmentPolicy", "LEGACY").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
//    HiveUtil.openCompression(sparkSession) //开启压缩

    val ssc = sparkSession.sparkContext
    import sparkSession.implicits._ //隐式转换
    ssc.textFile("/origin_data/salecourse.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val courseid = jsonObject.getString("courseid")
        val coursename = jsonObject.getString("coursename")
        val status = jsonObject.getString("status")
        val pointlistid = jsonObject.getString("pointlistid")
        val majorid = jsonObject.getString("majorid")
        val chapterid = jsonObject.getString("chapterid")
        val chaptername = jsonObject.getString("chaptername")
        val edusubjectid = jsonObject.getString("edusubjectid")
        val edusubjectname = jsonObject.getString("edusubjectname")
        val teacherid = jsonObject.getString("teacherid")
        val teachername = jsonObject.getString("teachername")
        val coursemanager = jsonObject.getString("coursemanager")
        val money = jsonObject.getString("money")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, coursename, status, pointlistid, majorid, chapterid, chaptername,
          edusubjectid, edusubjectname, teacherid, teachername, coursemanager, money, dt, dn)
      })
    }).toDF().coalesce(6).write.mode(SaveMode.Append).insertInto("spark_tuning.dwd_sale_course")

    ssc.textFile("/origin_data/coursepay.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val orderid = jsonObject.getString("orderid")
        val paymoney = jsonObject.getString("paymoney")
        val discount = jsonObject.getString("discount")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (orderid, discount, paymoney, createtime, dt, dn)
      })
    }).toDF().coalesce(6).write.mode(SaveMode.Append).insertInto("spark_tuning.dwd_course_pay")


    ssc.textFile("/origin_data/courseshoppingcart.log")
      .filter(item => {
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val courseid = jsonObject.getString("courseid")
        val orderid = jsonObject.getString("orderid")
        val coursename = jsonObject.getString("coursename")
        val discount = jsonObject.getString("discount")
        val sellmoney = jsonObject.getString("sellmoney")
        val createtime = jsonObject.getString("createtime")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (courseid, orderid, coursename, discount, sellmoney, createtime, dt, dn)
      })
    }).toDF().coalesce(6).write.mode(SaveMode.Append).insertInto("spark_tuning.dwd_course_shopping_cart")
  }

}
