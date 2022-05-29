package test

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties

object MyClickhouseUtil {
  val CLICKHOUSE_URL = "jdbc:clickhouse://chproxy.bdp.yxqiche.com:80/yx_ads"
  def executeSQL(sql:String): Unit ={
    //ru.yandex.clickhouse.ClickHouseDriver
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    val connection: Connection = DriverManager.getConnection(CLICKHOUSE_URL)
    val statement: Statement = connection.createStatement()
    statement.execute(sql)
    statement.close()
    connection.close()
  }
}
