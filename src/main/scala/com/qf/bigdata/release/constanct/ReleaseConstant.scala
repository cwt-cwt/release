package com.qf.bigdata.release.constanct

import org.apache.spark.storage.StorageLevel

/**
  * 常量
  */
object ReleaseConstant {

  //partition
  //Storage存储内存，
  // MEMORY_AND_DISK内存加磁盘
  //缓存内存满了，新的内存数据会把旧的剔除掉，优先选内存，没有内存会放在磁盘，不会丢数据
  val SEF_STORAGE_LEVEL =StorageLevel.MEMORY_AND_DISK
  //将分区传入
  val DEF_PARTITION:String = "bdp_day"
  //重分区
  val DEF_SOURCE_PARTITION = 4

  //维度列
  //封装的获取的数据的条件，数据要等同于该字段，类似于01
  val COL_RELEASE_SESSION_STATUS:String ="release_status"


//------------------ods-----------
  //写库名.表名，不写库名会转到默认的库中
  val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

  //-----------------DW-------------
  val DW_RELEASE_CUSTOMER="dwd_release.dw_release_customer"
}
