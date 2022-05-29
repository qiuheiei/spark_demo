package spark_tuning

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object PartitionTuning {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "lsh")
    val sparkConf = new SparkConf().setAppName("test")
//      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
//      .set("spark.sql.shuffle.partitions", "36")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
//    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
//    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
    testJoin(sparkSession)
  }

  def testJoin(sparkSession: SparkSession) = {
    //查询出三张表 并进行join 插入到最终表中
     sparkSession.sql(
       """
         |insert overwrite table spark_tuning.dws_salecourse_detail
         |select
         |      t1.courseid
         |    , t1.coursename
         |    , status
         |    , pointlistid
         |    , majorid
         |    , chapterid
         |    , chaptername
         |    , edusubjectid
         |    , edusubjectname
         |    , teacherid
         |    , teachername
         |    , coursemanager
         |    , money
         |    , t1.orderid
         |    , t1.discount as cart_discount
         |    , sellmoney
         |    , t1.createtime as cart_createtime
         |    , t3.discount pay_discount
         |    , paymoney
         |    , t3.createtime as pay_createtime
         |    , t1.dt
         |    , t1.dn
         |from spark_tuning.dwd_course_shopping_cart t1
         |left join  spark_tuning.dwd_sale_course t2 on t1.courseid = t2.courseid and t1.dt = t2.dt and t1.dn = t2.dn
         |left join spark_tuning.dwd_course_pay t3 on t1.orderid = t3.orderid and t1.dt = t3.dt and t1.dn = t3.dn
         |""".stripMargin)
//      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }

  /**
    * 打散大表  扩容小表 解决数据倾斜
    *
    * @param sparkSession
    */
  def testJoin2(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val saleCourse = sparkSession.sql("select *from dwd.dwd_sale_course")
    val coursePay = sparkSession.sql("select * from dwd.dwd_course_pay")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    val courseShoppingCart = sparkSession.sql("select *from dwd.dwd_course_shopping_cart")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")
    //将大表打散  打散36份
    val newCourseShoppingCart = courseShoppingCart.mapPartitions((partitions: Iterator[Row]) => {
      partitions.map(item => {
        val courseid = item.getAs[Int]("courseid")
        val randInt = Random.nextInt(36)
        DwdCourseShoppingCart(courseid, item.getAs[String]("orderid"),
          item.getAs[String]("coursename"), item.getAs[java.math.BigDecimal]("cart_discount"),
          item.getAs[java.math.BigDecimal]("sellmoney"), item.getAs[java.sql.Timestamp]("cart_createtime"),
          item.getAs[String]("dt"), item.getAs[String]("dn"), randInt + "_" + courseid)
      })
    })
    //小表进行扩容 扩大36倍
    val newSaleCourse = saleCourse.flatMap(item => {
      val list = new ArrayBuffer[DwdSaleCourse]()
      val courseid = item.getAs[Int]("courseid")
      val coursename = item.getAs[String]("coursename")
      val status = item.getAs[String]("status")
      val pointlistid = item.getAs[Int]("pointlistid")
      val majorid = item.getAs[Int]("majorid")
      val chapterid = item.getAs[Int]("chapterid")
      val chaptername = item.getAs[String]("chaptername")
      val edusubjectid = item.getAs[Int]("edusubjectid")
      val edusubjectname = item.getAs[String]("edusubjectname")
      val teacherid = item.getAs[Int]("teacherid")
      val teachername = item.getAs[String]("teachername")
      val coursemanager = item.getAs[String]("coursemanager")
      val money = item.getAs[java.math.BigDecimal]("money")
      val dt = item.getAs[String]("dt")
      val dn = item.getAs[String]("dn")
      for (i <- 0 until 36) {
        list.append(DwdSaleCourse(courseid, coursename, status, pointlistid, majorid, chapterid, chaptername, edusubjectid,
          edusubjectname, teacherid, teachername, coursemanager, money, dt, dn, courseid + "_" + i))
      }
      list
    })
    newSaleCourse.join(newCourseShoppingCart.drop("courseid").drop("coursename"),
      Seq("rand_courseid", "dt", "dn"), "right")
      .join(coursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }

  case class DwdCourseShoppingCart(courseid: Int,
                                   orderid: String,
                                   coursename: String,
                                   cart_discount: java.math.BigDecimal,
                                   sellmoney: java.math.BigDecimal,
                                   cart_createtime: java.sql.Timestamp,
                                   dt: String,
                                   dn: String,
                                   rand_courseid: String)

  case class DwdSaleCourse(courseid: Int,
                           coursename: String,
                           status: String,
                           pointlistid: Int,
                           majorid: Int,
                           chapterid: Int,
                           chaptername: String,
                           edusubjectid: Int,
                           edusubjectname: String,
                           teacherid: Int,
                           teachername: String,
                           coursemanager: String,
                           money: java.math.BigDecimal,
                           dt: String,
                           dn: String,
                           rand_courseid: String)

}
