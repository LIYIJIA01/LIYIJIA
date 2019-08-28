
package logDemo_3.TagUtils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagUtils_1 {
  //标签工具类
  val UserId=
    """
      | imei !='' or mac !='' or openudid !='' or androidid !='' or idfa !='' or
      | imeimd5 !='' or macmd5 !='' or openudidmd5 !='' or androididmd5 !='' or idfamd5 !='' or
      | imeisha1 !='' or macsha1 !='' or openudidsha1 !='' or androididsha1 !='' or idfasha1 !=''
    """.stripMargin

  //去除唯一不为空的ID
 def getUserId(row:Row):String = {
   row match {
     case v if StringUtils.isNotBlank(v.getAs[String]("imei")) =>
       "IM"+v.getAs[String]("imei")
     case v if StringUtils.isNotBlank(v.getAs[String]("mac")) =>
       "MC"+v.getAs[String]("mac")
     case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) =>
       "ID"+v.getAs[String]("idfa")
     case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) =>
       "OD"+v.getAs[String]("openudid")
     case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) =>
       "AOD"+v.getAs[String]("androidid")
     case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) =>
       "MD5IM"+v.getAs[String]("imeimd5")
     case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) =>
       "MD5MC"+v.getAs[String]("macmd5")
     case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) =>
       "MD5OD"+v.getAs[String]("openudidmd5")
     case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) =>
       "MD5AOD"+v.getAs[String]("androididmd5")
     case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) =>
       "MD5ID"+v.getAs[String]("idfamd5")
     case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) =>
       "SHIM"+v.getAs[String]("imeisha1")
     case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) =>
       "SHMC"+v.getAs[String]("macsha1")
     case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) =>
       "SHID"+v.getAs[String]("idfasha1")
     case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) =>
       "SHOD"+v.getAs[String]("openudidsha1")
     case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) =>
       "SHAOD"+v.getAs[String]("androididsha1")
   }

 }


}
