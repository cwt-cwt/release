package com.qf.bigdata.release.etl.release.dm

import scala.collection.mutable.ArrayBuffer

object DMReleaseColumnsHelper {
  /**
    * 目标客户
    */

  def selectDWReleaseCustomerCustomers():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    //获取DW所有数据
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("age")
    columns.+=("getAgeRange(age) as age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("ct")
    columns.+=("bdp_day")
  }

  /**
    * 渠道指标列
    *
    */
  def selectDMCustomerSourceColums():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
  }


}
