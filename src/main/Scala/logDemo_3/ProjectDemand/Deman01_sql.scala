package logDemo_3.ProjectDemand

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Deman01_sql {
  def main(args: Array[String]): Unit = {

    //sparksession读取已经切分好的数据

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val logs: DataFrame = spark.read.parquet("D:\\output1")
    logs.createOrReplaceTempView("log")
    spark.sql("select * from log limit 20").show()

    //地域
    spark.sql("select " +
      "provincename," +
      "cityname," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 1 then 1 else 0 end) `原始请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) `有效请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE = 3 then 1 else 0 end) `广告请求数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) `参与竞价数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) `参与竞价数`," +
      "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) `展示数`," +
      "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) `点击数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告消费`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告成本`" +
      "from log group by provincename,cityname").show()


      //运行商终端
    spark.sql("select ispname,ispid,"+
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 1 then 1 else 0 end) `原始请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) `有效请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE = 3 then 1 else 0 end) `广告请求数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) `参与竞价数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) `参与竞价数`," +
      "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) `展示数`," +
      "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) `点击数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告消费`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告成本`" +
      "from log group by ispname,ispid").show()

    //网络类型

    spark.sql("select networkmannername,"+
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 1 then 1 else 0 end) `原始请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) `有效请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE = 3 then 1 else 0 end) `广告请求数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) `参与竞价数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) `参与竞价数`," +
      "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) `展示数`," +
      "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) `点击数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告消费`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告成本`" +
      "from log group by networkmannername").show()

    //设备类

    spark.sql("select case devicetype when 1 then '手机' when 2 then '平板' else '其他' end `设备类型` ,"+
     "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 1 then 1 else 0 end) `原始请求数`," +
     "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) `有效请求数`," +
     "sum(case when REQUESTMODE = 1 and PROCESSNODE = 3 then 1 else 0 end) `广告请求数`," +
     "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) `参与竞价数`," +
     "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) `参与竞价数`," +
     "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) `展示数`," +
     "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) `点击数`," +
     "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告消费`," +
     "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告成本`" +
   "from log group by devicetype").show()


    //操作类
    spark.sql("select case client when 1 then 'andriod' when 2 then 'IOS' else 'WP' end `设备类型` ,"+
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 1 then 1 else 0 end) `原始请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) `有效请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE = 3 then 1 else 0 end) `广告请求数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) `参与竞价数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) `参与竞价数`," +
      "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) `展示数`," +
      "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) `点击数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告消费`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告成本`" +
      "from log group by client").show()


    //媒体类型

    spark.sql("select case appname ,"+
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 1 then 1 else 0 end) `原始请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) `有效请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE = 3 then 1 else 0 end) `广告请求数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) `参与竞价数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) `参与竞价数`," +
      "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) `展示数`," +
      "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) `点击数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告消费`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告成本`" +
      "from log group by mediatype").show()
    spark.close()

  }

}
