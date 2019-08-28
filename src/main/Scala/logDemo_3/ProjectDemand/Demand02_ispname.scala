package logDemo_3.ProjectDemand

import logDemo_3.TagUtils.RptUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Demand02_ispname {
  def main(args: Array[String]): Unit = {

    //判断路径是否正确
    if (args.length != 2) {
      println("目录参数不正确，退出程序！！！")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    //  val Array(inputPath, outputPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
    // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //获取数据
    val df: DataFrame = sQLContext.read.parquet("D:\\output1")
    //将数据进行处理 统计各个指标

    //注册临时表
    //df.registerTempTable("log")

    //指标统计
    //val result = sQLContext.sql("select * from log limit 10").show()

    val line: Unit =df.rdd.map(row => {
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
      val ispname= row.getAs[String]("ispname")

      //创造三个对应的方法处理九个指标

      //List(((pro, city)), requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      //List((ispname), requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      val reqlist: List[Double] = RptUtils.request(requestmode, processnode)
      val clicklist: List[Double] = RptUtils.click(requestmode, iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      ((ispname), reqlist ++ clicklist ++ adlist)
    }).reduceByKey(
      (List1, List2) => {
        List1.zip(List2).map(t => t._1+ t._2)
      })
      //整理元素
      .map(t =>{
      t._1+","+t._2.mkString(",")
    }).saveAsTextFile("D:\\output3")


    //如果存入mysql的话，需要实现使用foreachPartition
    //需要自己写一个连接池
    //作业
    //地域指标实现一个SQL版的

  }
}
