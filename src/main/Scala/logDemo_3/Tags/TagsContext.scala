package logDemo_3.Tags

import logDemo_3.TagUtils.TagUtils_1
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TagsContext {
  //所有标签总和
  def main(args: Array[String]): Unit = {

//    if(args.length !=2){
//      print("目录不匹配,退出程序")
//      sys.exit()
//    }
//    val Array(inputpath,outputpath,dicpath,stopwords)=args
    //创建上下文
    val conf=new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark= SparkSession.builder().config(conf).getOrCreate()

    //读取数据
    val datas: DataFrame =spark.read.parquet("D:\\output1")

    //读取文件并过滤
    val dicmap =spark.read.textFile("D:\\input\\app_dict.txt").rdd.map(_.split("\t",-1))
      .filter(_.length>=5).map(arr=>{
        (arr(4),arr(1))}).collect.toMap

    //将处理好的数据进行广播
     val bdAppName= spark.sparkContext.broadcast(dicmap)

     //获取停用词库
    val stopwordDir: Map[String, Int] =spark.read.textFile("D:\\input\\stopwords.txt").rdd.map((_,0)).collect().toMap
    val bdstopwordsDic = spark.sparkContext.broadcast(stopwordDir)


    //过滤符合ID的数据
    val data: Dataset[Row] =datas.filter(TagUtils_1.UserId )
    //接下来所有的标签都是在内部实现
    data.rdd.map(row=>{
      //取出用户id
      //val userId=TagUtils_1.getUserId(row)
      //根据每一种数据,打上对应的标签
      //val adTag=TagsAd.makeTags(row)

      //关键词
       val keyword = TagsKeyWord.makeTags(row)

      //通过row数据,打上所有标签
     // val adList =TagsAd.makeTags(row, bdstopwordsDic)
      keyword
    }).saveAsTextFile("D:\\output6")


    spark.stop()

  }

}
