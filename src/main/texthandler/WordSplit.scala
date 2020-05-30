package texthandler

import com.huaban.analysis.jieba.JiebaSegmenter
import scala.collection.JavaConverters._
object WordSplit {

  def jiebaCut(sentence:String): String ={
    val line = sentence.replaceAll("[^\u4e00-\u9fa5]","")
    val words = new JiebaSegmenter().sentenceProcess(line).asScala
    words.map(s=>s.replaceAll("\\s+","")).filter(s=>s.length!=0).mkString(" ")
  }

  def main(args: Array[String]): Unit = {
    println(jiebaCut("为中华之崛起而读书"))
  }

}
