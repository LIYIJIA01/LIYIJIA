package logDemo_3.TagUtils

object Utils2Type {
  def toInt(str:String):Int={
   try{str.toInt
   } catch {
     case _:Exception=>0
   }

  }
  //将读取的数据（String）类型转换成Int
  def toDouble(str:String):Double={
    try{str.toDouble
    } catch {
      case _:Exception=>0
    }
  }

}
