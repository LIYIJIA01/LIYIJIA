package ProjectExam

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

object Tags_1 {

  def main(args: Array[String]): Unit = {

    var list: List[List[String]] = List()
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //读取数据
    val log: Dataset[String] = spark.read.textFile("D:\\input\\json.txt")


    //创建空集合 保存解析后的json数据
    val logs: mutable.Buffer[String] = log.rdd.collect().toBuffer

    //遍历数组 讲解析后的json数据保存到数组中
    for(i <- 0 until logs.length){
      val str: String = logs(i).toString

      //解析文件
      val jsonparse: JSONObject = JSON.parseObject(str)

      val status = jsonparse.getIntValue("status")
      if(status == 0) return ""

      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

      val poisArray = regeocodeJson.getJSONArray("pois")
      if(poisArray == null || poisArray.isEmpty) return null

      // 创建集合 保存数据
      val buffer = collection.mutable.ListBuffer[String]()

      // 循环输出
      for(item <- poisArray.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }

      val list1: List[String] = buffer.toList
      println("*************************************")


      list:+=list1 //集合拼接
    }
    println(list)

    val res: List[(String, Int)] = list.flatMap(x => x)
                  .filter(x => x != "[]").map(x => (x, 1))
                  .groupBy(x => ("Businessarea:"+x._1))
                  .mapValues(x => x.size).toList

      res.foreach(println)

      spark.stop()

  }

}
