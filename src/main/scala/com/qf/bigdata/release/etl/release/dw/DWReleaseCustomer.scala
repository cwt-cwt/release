package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.constanct.ReleaseConstant
import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class DWReleaseCustomer {

}

/**
  * DW投放目标主题客户
  */
object DWReleaseCustomer {

  //日志处理
  private val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

  /**
    * 目标客户
    * 01
    */

  //传入（spark[封装spark session注册udf]，appname，分区） 无返回值
  def handleReleaseJob(spark: SparkSession, appName: String, bdp_day: String): Unit = {
    //获取当前时间
    //可以打印当前的job执行了多长时间，开始时间-结束时间
    val begin = System.currentTimeMillis()
    //要打印log，所以抛出异常
    try {
      //导入隐式转换
      import spark.implicits._
      //sparksql的内置函数包，写dsl风格要导入，需要手动调，不会自动调入
      import org.apache.spark.sql.functions._

      //设置缓存级别，做缓存表会用，主题表单一不会和其他表产生关联，不用缓存
      //缓存放在存储内存中
      //常量类中具体定义，调用过来
      val storagelevel = ReleaseConstant.SEF_STORAGE_LEVEL

      //存储方式，覆盖
      val saveMode = SaveMode.Overwrite

      //获取日志字段数据
      //获取日志字段封装起来，调用过来
      //封装内容在dw层 投放业务列表
      //获取到ods字段数据，获取的是一个分区的数据
      val customerColumns = DWReleaseColumnsHelper.selectDWReleaseCustomerColumns()

      //获取的是分区的数据，要设置
      //设置条件当天数据，获取目标客户01
      //col取字段
      //ReleaseConstant.DEF_PARTITION传常量，类似于col(s"${bdp_day}")
      //lit自变量，要判断获取的分区时间是否等于传进来的dap_day，把dap_day用lit包住
      //lit可以传任何类型，最后返回一个列 col返回也是列  列和列要用===
      val customerReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
        //需要满足另一个条件状态=01
        and
        //将状态封装为常量，类似于col(s"${release_status}")
        col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}")
          //将获取的状态和封装的ReleaseStatusEnum.CUSTOMER中的状态相比
          //getcode是封装类中的第一个内容，表示状态
          === lit(ReleaseStatusEnum.CUSTOMER.getCode))
      //(spark,ReleaseConstant.ODS_RELEASE_SESSION[数据库表],数据)
      val customerReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.ODS_RELEASE_SESSION, customerColumns)
        //填入条件
        .where(customerReleaseCondition)
        //重分区，调用封装设置的分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITION)
      println("DWReleaseDF------------")
      //打印查看结果
      customerReleaseDF.show(10, false)
      //目标用户（存储），封装到spark工具类
      //(dataframe,表名,存储方式)
      //表名引用了常量ReleaseConstant.DW_RELEASE_CUSTOMER
      SparkHelper.writeTableData(customerReleaseDF, ReleaseConstant.DW_RELEASE_CUSTOMER, saveMode)


    } catch {

      case ex: Exception => {
        logger.error(ex.getMessage, ex)
      }
    } finally {
      //任务处理时长
      //当前时间-开始处理时间
      s"任务处理时长:${appName},bdp_day = ${bdp_day},${System.currentTimeMillis() - begin}"
    }
  }

  /**
    * 投放目标用户
    *
    */
  def handleJobs(appName: String, bdp_day_begin: String, bdp_day_end: String): Unit = {
    //可能会出现参数异常
    var spark: SparkSession = null
    try {
      //配置spark参数
      val conf = new SparkConf()
        //设置hive参数，基于spark的
        //开启动态分区
        .set("hive.exec.dynamic.partition", "true")
        //设置不严格模式
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        //每个partition的分区容量
        .set("spark.sql.shuffle.partitions", "32")
        //合并小文件
        .set("hive.merge.mapfiles", "true")
        //输入io形式
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        //资源大小
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")

        .setAppName(appName)
        .setMaster("local[*]")

      //创建上下文，调用封装的spark
      spark = SparkHelper.createSpark(conf)
      //解析参数,写个封装参数的方法，在spark工具类中
      //传入的时间参数，字符串转时间
      val timeRange = SparkHelper.rangeDates(bdp_day_begin,bdp_day_end)
      //循环参数
      for (bap_day <- timeRange){
        val bdp_date = bap_day.toString
        handleReleaseJob(spark,appName,bdp_date)

      }
    } catch {
      case ex:Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = "dw_release_job"
    val bdp_day_begin = "2019-09-24"
    val bdp_day_end = "2019-09-24"
    //执行job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }

}
