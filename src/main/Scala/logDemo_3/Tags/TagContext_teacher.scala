package com.Tags

import com.typesafe.config.ConfigFactory
import logDemo_3.TagUtils.TagUtils_1
import logDemo_3.Tags.{TagBusiness, TagsAd, TagsApp, TagsClient, TagsKeyWord}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagContext_teacher {
  def main(args: Array[String]): Unit = {

//    if (args.length != 5) {
//      println("目录不匹配，退出程序")
//      sys.exit()
//    }

//    val Array(inputPath, outputPath, dirPath, stopPath, days) = args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    // todo 调用Hbase API
    // 加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.host"))
    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    // 判断表是否可用
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

    // 读取数据
    val df = sQLContext.read.parquet("D:\\output1")
    // 读取字段文件
    val map = sc.textFile("D:\\input\\app_dict.txt").map(_.split("\t", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
    // 将处理好的数据广播
    val broadcast = sc.broadcast(map)

    // 获取停用词库
    val stopword = sc.textFile("D:\\input\\stopwords.txt").map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)
    // 过滤符合Id的数据
    val data: Dataset[Row] =df.filter(TagUtils_1.UserId)

    // 接下来所有的标签都在内部实现
    // 过滤符合Id的数据
    val baseRDD = df.filter(TagUtils_1.UserId)
    data.rdd.map(row=>{
      //取出用户id
      val userId=TagUtils_1.getUserId(row)
      //根据每一种数据,打上对应的标签
      //val adTag=TagsAd.makeTags(row)

      //关键词
      val keyword = TagsKeyWord.makeTags(row,bcstopword)

      //通过row数据,打上所有标签
      // val adList =TagsAd.makeTags(row, bdstopwordsDic)
      (userId,row)
      keyword
    }).saveAsTextFile("D:\\output6")

    //构建点集合


    sc.stop()
  }
}