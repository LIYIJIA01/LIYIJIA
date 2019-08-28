package logDemo_3.TagUtils

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/*
http请求协议
 */
object  HttpUtils {
  //GET请求

  def get(url:String):String={
    val clinet=HttpClients.createDefault()
    val get = new HttpGet(url)

    //发送请求
    val respone:CloseableHttpResponse = clinet.execute(get)

    //获取返回结果
    EntityUtils.toString(respone.getEntity,"UTF-8")



  }

}
