package logDemo_3.TagUtils

import logDemo_3.Tags.TagBusiness
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {
  /*
  商圈类测试类-->调用TagBusiness
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val data: DataFrame = spark.read.parquet("D:\\output1")
    data.rdd.map(row=>{

      val business = TagBusiness.makeTags(row)
      business
    }).foreach(println)

  }

}
