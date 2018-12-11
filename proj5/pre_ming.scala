//
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import java.util.BitSet

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
object pre_ming {
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("俩个参数")
    return
    }
    val conf=new SparkConf().setAppName("频繁项集挖掘")
    val sc = new SparkContext(conf)
    val K = 6
    val transactions = sc.textFile(args(0)).map(line =>((line, 1))).reduceByKey(_ + _).map(line => {
      val bitSet = new BitSet()
      val ss = line._1.split(" ")
      for (i <- 0 until ss.length) {
        bitSet.set(ss(i).toInt, true)
      }
      (bitSet, line._2)
    }).cache()
    val TRANSACITON_NUM=sc.textFile((args(0))).count().toFloat
    val SUPPORT_NUM=TRANSACITON_NUM*0.8
    //transactions.saveAsTextFile((args(1)+"/tra"))
//用bitset减少存储开销，损失出现次数
    var fi = transactions.flatMap { line =>
      var tmp = new ArrayBuffer[(String, Int)]()
      for (i <- 0 until line._1.size()) {
        if (line._1.get(i)) tmp+=((i.toString, line._2))
      }
      tmp
    }.reduceByKey(_ + _).filter(_._2 >= SUPPORT_NUM).cache()
    val result = fi.map(line => ("["+line._1 +"]"+ ":" +line._2/TRANSACITON_NUM))
    result.saveAsTextFile(args(1) + "/min_supp-1")


    for (i <- 2 to K) {
      val candiateFI = CandiateFI(fi.map(_._1).collect(), i) //得到一个stringlist
      val bccFI = sc.broadcast(candiateFI)//将k+1候选项生成一个广播变量
      fi = transactions.flatMap { line =>
        val tmp = new ArrayBuffer[(String, Int)]()
        bccFI.value.foreach { itemset =>
          val itemArray = itemset.split(",")
          var count = 0
          for (item <- itemArray) {if (line._1.get(item.toInt)){count += 1}  //判断候选项是否在bitset中
          if (count == itemArray.size){ tmp += ((itemset, line._2)) }} //都出现才认为是频繁项
        }
        tmp
      }.reduceByKey(_ + _).filter(_._2>= SUPPORT_NUM).cache()
      val result = fi.map(line =>("["+line._1+"]"+ ":" + line._2/TRANSACITON_NUM))
      result.saveAsTextFile(args(1) + "/min_supp-" + i)
      bccFI.unpersist()
    }
  }
  def CandiateFI(f: Array[String], tag: Int) = {
    val separator = ","
    val arrayBuffer = ArrayBuffer[String]()
    var hasInfrequentSubItem = false
    for(i <- 0 until f.length;j <- i + 1 until f.length){
      var tmp = ""
      if(2 == tag) tmp = (f(i) + "," + f(j)).split(",").sortWith((a,b) => a.toInt <= b.toInt).reduce(_+","+_)
      else {
        if (f(i).substring(0, f(i).lastIndexOf(',')).equals(f(j).substring(0, f(j).lastIndexOf(',')))) {
          tmp = (f(i) + f(j).substring(f(j).lastIndexOf(','))).split(",").sortWith((a, b) => a.toInt <= b.toInt).reduce(_ + "," + _)
        }
      }
      if (!tmp.equals("")) {
        val arrayTmp = tmp.split(separator)
        breakable {
          for (i <- 0 until arrayTmp.size) {
            var subItem = ""
            for (j <- 0 until arrayTmp.size) {
              if (j != i) subItem += arrayTmp(j) + separator
            }
            subItem = subItem.substring(0, subItem.lastIndexOf(separator))
            if (!f.contains(subItem)) {
              hasInfrequentSubItem = true
              break
            }
          }
        }
      }
      else hasInfrequentSubItem = true
      if (!hasInfrequentSubItem) arrayBuffer += (tmp)
    }
    arrayBuffer.toArray
  }
}
