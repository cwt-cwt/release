package com.qf.bigdata.release.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * spark工具类
  */
object SparkHelper {

  private val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)

  /**
    * 读取数据
    */
  def readTableData(spark:SparkSession,tableName:String,colName:mutable.Seq[String]):DataFrame = {
    //导入隐式转换
    import spark.implicits._
    //获取数据，读表名，返回一个dataframe
    val tableDF = spark.read.table(tableName)
      //selectExpr解析内部的每一个元素，会把每一个元素当成一个列，不会当成单一的字符串
      //多个字符串，可以用*代表数组中每一个元素
      .selectExpr(colName:_*)
    tableDF
  }

  /**
    * 写入数据
    * 目标存储
    */
  //(dataframe,表名,存储方式)
  def writeTableData(sourceDF:DataFrame,table:String,mode:SaveMode):Unit = {
    //写入表
    sourceDF.write.mode(mode).insertInto(table)
  }

  /**
    * 创建SparkSession
    */
  def createSpark(conf: SparkConf): SparkSession = {
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    //加载自定义函数

    spark
  }

  /**
    * 参数的校验
    */
  def rangeDates(begin: String, end: String): Seq[String] = {
    val bdp_days = new ArrayBuffer[String]()
    try {
      //取当前时间

      //转为时间格式,写个工具类
      val bdp_date_begin = DateUtil.dateFromat4String(begin, "yyyy-MM-dd")
      val bdp_date_end = DateUtil.dateFromat4String(end, "yyyy-MM-dd")
      //开始时间可能和结束时间相等
      //判断两个时间是否相等
      //如果相等
      if (begin.equals(end)) {
        //相等就取第一个开始时间
        bdp_days.+=(bdp_date_begin)
      } else {
        //不相等，计算时间差
        var cday = bdp_date_begin
        while (cday != bdp_date_end) {
          //加到数组中
          bdp_days.+=(cday)
          //处理差值,让可变初始时间进行累加  以天为单位
          val pday = DateUtil.dateFromat4StringDiff(cday, 1)

          cday = pday
        }

      }
    } catch {
      //如果时间格式不匹配就跳过
      case ex: Exception => {
        println("参数不匹配")
        //当前log的信息和位置
        logger.error(ex.getMessage, ex)
      }

    }
    bdp_days
  }
}
