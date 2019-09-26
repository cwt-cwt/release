package com.qf.bigdata.release.etl.release.udf

import com.qf.bigdata.release.util.CommonUtil


/**
  * spark UDF
  */
object QFUdf {
  /**
    * 年龄段
    *
    */
  def getAgeRange(age:String):String={
    var tseg=""
    try{
      tseg = CommonUtil.getAgeRange(age)

    }catch {
      case ex:Exception=>{
        println(s"$ex")
      }
    }
    tseg
  }

}
