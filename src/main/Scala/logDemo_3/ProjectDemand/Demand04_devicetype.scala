package logDemo_3.ProjectDemand

import logDemo_3.TagUtils.RptUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demand04_devicetype {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    //创建执行入口
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //获取数据
    val logs: DataFrame = spark.read.parquet("D:\\output1")

    val line: Unit =logs.rdd.map(row => {
      //把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")

      val processnode = row.getAs[Int]("processnode")

      val iseffective = row.getAs[Int]("iseffective")

      val isbilling = row.getAs[Int]("isbilling")

      val isbid = row.getAs[Int]("isbid")

      val iswin = row.getAs[Int]("iswin")

      val adorderid = row.getAs[Int]("adorderid")

      val winprice = row.getAs[Double]("winprice")

      val adpayment = row.getAs[Double]("adpayment")

      //将运营商的名字作为key值
      val devicetype= row.getAs[Int]("devicetype")

      //创造三个对应的方法处理九个指标

      //List(((pro, city)), requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      //List((ispname), requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      val devices: String =DevicetypeUtils.num1(devicetype)

      val reqlist: List[Double] = RptUtils.request(requestmode, processnode)
      val clicklist: List[Double] = RptUtils.click(requestmode, iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      ((devices), reqlist ++ clicklist ++ adlist)
    }).reduceByKey(
      (List1, List2) => {
        List1.zip(List2).map(t => t._1+ t._2)
      })
      //整理元素
      .map(t =>{
      t._1+","+t._2.mkString(",")
    }).coalesce(1)saveAsTextFile("D:\\output5")

    spark.close()
  }

}
