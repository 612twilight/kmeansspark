package texthandler

import com.alibaba.fastjson.JSON
import texthandler.WordSplit.jiebaCut

object JsonReader {
  def generateFromJson(line: String): (String, (String, String, String)) = {
    val originText = JSON.parseObject(line).getString("text")
    val id = JSON.parseObject(line).getString("id")
    val resource = JSON.parseObject(line).getString("resource")
    val splitWords = jiebaCut(originText)
    (id, (splitWords, resource, originText))
  }

  def main(args: Array[String]): Unit = {
    println(generateFromJson("{\"text\":\"世界人民大团结万岁\",\"id\":\"3\",\"resource\":\"百度\"}"))
  }

}
