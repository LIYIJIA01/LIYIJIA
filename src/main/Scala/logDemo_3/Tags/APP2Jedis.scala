package logDemo_3.Tags

import logDemo_3.TagUtils.JedisConnectionPool
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, sql}

object APP2Jedis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //讀取字段
    val dict: Dataset[String] =spark.read.textFile("D:\\input\\app_dict.txt")

    //讀取字段文件
    dict.rdd.map(_.split("\t",-1))
      .filter(_.length>=5).foreachPartition(arr=>{
      val jedis = JedisConnectionPool.getConnection()
      arr.foreach(arr=>{
        jedis.set(arr(4),arr(1))
      })
      jedis.close()
    })
  spark.stop()

  }

}
