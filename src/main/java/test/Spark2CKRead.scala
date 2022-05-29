package test

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark2CKRead {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test-clickhouse")//.setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val CLICKHOUSE_URL = "jdbc:clickhouse://chproxy.bdp.yxqiche.com:80/yx_ads"

    val tablename = s" (select * from api_proce_log2  limit 1 )"
    //  val dropTableSQL:String = s"drop table if exists $userDB.$tableName"
    val readDataDf = spark.read
      .format("jdbc")
      .option("url",CLICKHOUSE_URL)
      .option("fetchsize", "100")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("user", "yixin_etl")
      .option("password", "jxykHBKR")
      .option("dbtable", tablename)
      .load()
    readDataDf.show()
  }


}


