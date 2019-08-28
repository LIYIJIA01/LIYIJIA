package logDemo_3.ProjectDemand

object DevicetypeUtils {
  def num1(devicetype:Int):String={
    // devicetype when 1 then '手机' when 2 then '平板' else '其他'
    if(devicetype ==1){
      "手机"
    }else if(devicetype==2){
      "平板"
    }else{
      "其他"
    }
  }

}
