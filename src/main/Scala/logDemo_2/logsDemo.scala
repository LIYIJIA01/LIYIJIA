//package logDemo_2
//
//import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
//import org.apache.spark.{SparkConf, sql}
//
//object logsDemo {
//  def main(args: Array[String]): Unit = {
//
//    //sparksession读取数据
//
//    val conf = new SparkConf().setAppName("日志数据分析").setMaster("local[2]")
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//
//
//    val logs: Dataset[String] = spark.read.textFile("D:\\input\\2016-10-01_06_p1_invalid.1475274123982.log")
//
//    import spark.implicits._
//    val line: Dataset[Row] = logs.map(t=>t.split(",",t.length)).filter(_.length >= 85).map(arr=>{Row(
//      arr(0),
//      Utils2Type(arr(1)),
//      Utils2Type(arr(2)),
//      Utils2Type(arr(3)),
//      Utils2Type(arr(4)),
//      arr(5),
//      arr(6),
//      Utils2Type(arr(7)),
//      Utils2Type(arr(8)),
//      Utils3Type(arr(9)),
//      Utils3Type(arr(10)),
//      arr(11),
//      arr(12),
//      arr(13),
//      arr(14),
//      arr(15),
//      arr(16),
//      Utils2Type(arr(17)),
//      arr(18),
//      arr(19),
//      Utils2Type(arr(20)),
//      Utils2Type(arr(21)),
//      arr(22),
//      arr(23),
//      arr(24),
//      arr(25),
//      Utils2Type(arr(26)),
//      arr(27),
//      Utils2Type(arr(28)),
//      arr(29),
//      Utils2Type(arr(30)),
//      Utils2Type(arr(31)),
//      Utils2Type(arr(32)),
//      arr(33),
//      Utils2Type(arr(34)),
//      Utils2Type(arr(35)),
//      Utils2Type(arr(36)),
//      arr(37),
//      Utils2Type(arr(38)),
//      Utils2Type(arr(39)),
//      Utils3Type(arr(40)),
//      Utils3Type(arr(41)),
//      Utils2Type(arr(42)),
//      arr(43),
//      Utils3Type(arr(44)),
//      Utils3Type(arr(45)),
//      arr(46),
//      arr(47),
//      arr(48),
//      arr(49),
//      arr(50),
//      arr(51),
//      arr(52),
//      arr(53),
//      arr(54),
//      arr(55),
//      arr(56),
//      Utils2Type(arr(57)),
//      Utils3Type(arr(58)),
//      Utils2Type(arr(59)),
//      Utils2Type(arr(60)),
//      arr(61),
//      arr(62),
//      arr(63),
//      arr(64),
//      arr(65),
//      arr(66),
//      arr(67),
//      arr(68),
//      arr(69),
//      arr(70),
//      arr(71),
//      arr(72),
//      Utils2Type(arr(73)),
//      Utils3Type(arr(74)),
//      Utils3Type(arr(75)),
//      Utils3Type(arr(76)),
//      Utils3Type(arr(77)),
//      Utils3Type(arr(78)),
//      arr(79),
//      arr(80),
//      arr(81),
//      arr(82),
//      arr(83),
//      Utils2Type(arr(84))
//    )
//    })
//
//    line.createGlobalTempView("logs")
//    spark.sql("select sessionid,advertisersid from logs limit 10").show
//
//    spark.close()
//  }
//
//  //将读取的数据（String）类型转换成Int
//  def Utils2Type(str:String):Int={
//    try{str.toInt
//    } catch {
//      case _:Exception=>0
//    }
//
//  }
//  //将读取的数据（String）类型转换成Int
//  def Utils3Type(str:String):Double={
//    try{str.toDouble
//    } catch {
//      case _:Exception=>0
//    }
//  }
//
//
//}