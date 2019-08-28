package LogDemo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object LogDemo_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("日志数据分析").setMaster("local[2]")
    //val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = new SparkContext(conf)
    val sqlContext= new SQLContext(sc)

    val data: RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\项目阶段\\Spark用户画像分析\\2016-10-01_06_p1_invalid.1475274123982.log")
    //将RDD格式转变成DF格式
    val rowRDD: RDD[Row] = data.map(t=>t.split(",",t.length)).filter(_.length >= 85).map(arr=>{Row(
          arr(0),
          Utils2Type(arr(1)),
          Utils2Type(arr(2)),
          Utils2Type(arr(3)),
          Utils2Type(arr(4)),
          arr(5),
          arr(6),
          Utils2Type(arr(7)),
          Utils2Type(arr(8)),
          Utils3Type(arr(9)),
          Utils3Type(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          Utils2Type(arr(17)),
          arr(18),
          arr(19),
          Utils2Type(arr(20)),
          Utils2Type(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          Utils2Type(arr(26)),
          arr(27),
          Utils2Type(arr(28)),
          arr(29),
          Utils2Type(arr(30)),
          Utils2Type(arr(31)),
          Utils2Type(arr(32)),
          arr(33),
          Utils2Type(arr(34)),
          Utils2Type(arr(35)),
          Utils2Type(arr(36)),
          arr(37),
          Utils2Type(arr(38)),
          Utils2Type(arr(39)),
          Utils3Type(arr(40)),
          Utils3Type(arr(41)),
          Utils2Type(arr(42)),
          arr(43),
          Utils3Type(arr(44)),
          Utils3Type(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          Utils2Type(arr(57)),
          Utils3Type(arr(58)),
          Utils2Type(arr(59)),
          Utils2Type(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          Utils2Type(arr(73)),
          Utils3Type(arr(74)),
          Utils3Type(arr(75)),
          Utils3Type(arr(76)),
          Utils3Type(arr(77)),
          Utils3Type(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          Utils2Type(arr(84))
        )
      })
    // 构建DF
   val dataframe=sqlContext.createDataFrame(rowRDD,SchemaUtils.structtype)
    // 保存数据
    dataframe.write.parquet("D:/output")
    sc.stop()
  }


  //将读取的数据（String）类型转换成Int
  def Utils2Type(str:String):Int={
    try{str.toInt
    } catch {
      case _:Exception=>0
    }

  }
  //将读取的数据（String）类型转换成Int
  def Utils3Type(str:String):Double={
    try{str.toDouble
    } catch {
      case _:Exception=>0
    }
  }
}
