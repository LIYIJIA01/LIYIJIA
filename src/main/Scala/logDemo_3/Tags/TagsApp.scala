package logDemo_3.Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row


object TagsApp extends  Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    val row =args(0).asInstanceOf[Row]//格式转换
    val appdic = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    //获取appname appid
    val appname= row.getAs[String]("appname")
    val appid= row.getAs[String]("appid")

    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNotBlank(appid)){
      list:+=("APP"+appdic.value.getOrElse(appid,appid),1)
    }
    list
  }
}
