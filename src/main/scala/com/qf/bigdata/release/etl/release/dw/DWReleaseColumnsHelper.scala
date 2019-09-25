package com.qf.bigdata.release.etl.release.dw

import scala.collection.mutable.ArrayBuffer

/**
  * DW层 投放业务表
  */
object DWReleaseColumnsHelper {
  /**
    * 目标用户
    */
//将写好的sql放进来
//selectDWReleaseCustomerColumns() 获取字段，封装成数组
//写好后调用到主类获取ods数据
  def selectDWReleaseCustomerColumns():ArrayBuffer[String]={
  //将所有字段放在arraybuffer
    val columns=new ArrayBuffer[String]()
  //将字段内容放进数组
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$. idcard') as idcard")
  //将\\字符转义多写两个\\
    columns.+=("(cast(date_format(now(),' yyyy') as int) - cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\\\d{6})(\\\\d{4})',2) as int)) as avg")
    columns.+=("cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\\\d{16})(\\\\d{1})()',2) as int) % 2 as gender")
    columns.+=("get_json_object(exts,'$. area_code') as area_code")
    columns.+=("get_json_object(exts,'$. Longitude') as longitude")
    columns.+=("get_json_object(exts,'$. latitude') as latitude")
    columns.+=("get_json_object(exts,'$. matter_id') as matter_id")
    columns.+=("get_json_object(exts,'$. model_code') as model_code")
    columns.+=("get_json_object(exts,'$. model_version') as model_version")
    columns.+=("get_json_object(exts,'$. aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }
}
