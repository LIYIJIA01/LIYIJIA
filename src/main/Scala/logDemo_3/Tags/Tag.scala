package logDemo_3.Tags

trait Tag {
  //打标签的统一入口
  def makeTags(args:Any*):List[(String,Int)]


}
