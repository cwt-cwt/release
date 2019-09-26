package com.qf.bigdata.release.etl.release.dm

import com.qf.bigdata.release.constanct.ReleaseConstant
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class DMReleaseCustomer {

}

/**
  * 投放目标数据集市
  */
object DMReleaseCustomer {
  private val logger: Logger = LoggerFactory.getLogger(DMReleaseCustomer.getClass)

  /**
    * 统计目标客户集市
    */
  def handleReleaseJob(spark: SparkSession, appName: String, bdp_day: String): Unit = {
    //导入隐式转换和内置函数
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //缓存级别
    val saveMode = SaveMode.Overwrite

    val storangeLevel = ReleaseConstant.DEF_STORAGE_LEVEL
    //获取日志数据
    val customerColumns = DMReleaseColumnsHelper.selectDWReleaseCustomerCustomers()

    //获取当天数据
    val customerCondition = col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
    val customerReleaseDF = SparkHelper.readTableData(spark, ReleaseConstant.DW_RELEASE_CUSTOMER, customerColumns)
      .where(customerCondition)
      //对当前datafranme进行缓存，设置缓存级别
      .persist(storangeLevel)
    println("DW----------------------------------")

    customerReleaseDF.show(10, false)

    //统计渠道指标
    //把分组值放一起
    //s换成$,表示列
    val customerSourceGroupColnmus = Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCE}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")

    //插入列
    val customerSourceColumns = DMReleaseColumnsHelper.selectDMCustomerSourceColums()
    //按照需求分组进行聚合
    val customerSourceDMDF = customerReleaseDF.groupBy(customerSourceGroupColnmus: _*)
      //agg聚合
      .agg(
        countDistinct(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
          //alias起别名
        .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
        count(col(ReleaseConstant.COL_RELEASE_DEVICE_NUM))
        .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
    )

    //按照条件查询,没有则把第二个参数赋值给第一个
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      //所有维度列
      .selectExpr(customerSourceColumns:_*)

    //打印
    println("DM-------------")
    customerSourceDMDF.show(10,false)
    //写入hive
    SparkHelper.writeTableData(
      customerSourceDMDF,ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCE,saveMode)


  }

}
