package com.catl.config

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.util.control.Breaks._

/**
  * Created by cong.xiang on 2017/11/3.
  */
object DataCompress{
  var targetR = 0.99
  var current_sample_percentage = 0.1
  var sample_increase = 0.1

  def gospark(source_list: List[(Double, Double)],broadcastVar_list_config: Broadcast[List[String]]): List[(Double, Double)] = {
    //step1算法步骤:x为横坐标，y为纵坐标 ，先算出s的值并排序
    val source = source_list.sortBy(x => x._1).toArray //根据x排序
    val step1ResultS = for (i <- 1 until source.length - 1) yield {
      var x = source(i)._1//x值
      var y = source(i)._2//y值
      var xBefore = source(i - 1)._1//x上一个点值
      var yBefore = source(i - 1)._2//y上一个点值
      var xLast = source(i + 1)._1//x下一个点值
      var yLast = source(i + 1)._2//y下一个点值
      var s = (xLast - xBefore) * (y - yBefore) + (yBefore - yLast) * (x - xBefore) // S_i=(x_(i+1)-x_(i-1) )(y_i-y_(i-1) )+(y_(i-1)-y_(i+1) )(x_i-x_(i-1) ),i=2,…,n-1
      (x, y, if (s >= 0) s else -s)//取出s的绝对值
    }
    val Step1toBuffer = step1ResultS.toArray.toBuffer
    val resultABS = Step1toBuffer.sortWith(_._3 > _._3)//根据绝对值排序

    //各个参数赋值
    var targetR = broadcastVar_list_config.value(0).split(";")(5).toDouble //拟合度
    var current_sample_percentage = broadcastVar_list_config.value(0).split(";")(6).toDouble //百分比
    var sample_increase = broadcastVar_list_config.value(0).split(";")(7).toDouble //增加步长
    var R2 = 0.0
    var mark = 1
    var dataPercent = resultABS.slice(0, Math.floor(resultABS.size * 0.1).toInt)

    //step2 :算法逻辑参考数据压缩功能需求说明文档
    while (mark == 1) {
      //取出按照初始压缩比例取出集合，向下取整
      dataPercent = resultABS.slice(0, Math.floor(resultABS.size * current_sample_percentage).toInt)

      //开始计算，这边用到一个集合交集并集操作
      var a = source.map(x => (x._1, x._2))//全部值
      var b = dataPercent.map(x => (x._1, x._2))//比例值
      var k_diff = a.diff(b).map(x=>((x._1, x._2),0))//取出两者不同的区域，并标记为0，未选取点
      var k_inst = a.intersect(b).map(x=>((x._1, x._2),1))//取出两者相同的区域，并标记为1，选取点
      var k = k_diff.union(k_inst).sortBy(x => x._1._1)//上诉两者union，合并成新的集合
      k(0) = (k(0)._1, 1)//对集合第一个点，重新标记为1
      k(k.length - 1) = (k(k.length - 1)._1, 1)//对集合最后一个点，重新标记为1

      //根据上诉的标记值，找到最近的未选取点
      var shengYuAll = ArrayBuffer[(Double, Double, Double)]()
      for (i <- 0 until k.length) {
        var beforePoint = (0.0, 0.0) //临时变量前一点
        var lastPoint = (0.0, 0.0) //临时变量后一点
        //根据标记值，判定选取点和未选取点
        if (k(i)._2 == 0) {
          breakable {
            for (j <- 1 until i) {
              //找到最近的已选点下标前一点
              if (k(i - j)._2 != 0) {
                beforePoint = k(i - j)._1
                break
              }
            }
          }
          breakable {
            for (j <- i + 1 until k.length) {
              //找到最近的已选点下标后一点
              if (k(j)._2 != 0) {
                lastPoint = k(j)._1
                break
              }
            }
          }

          //开始计算k，值，做一个简单判断
          var xyk_fenmu = lastPoint._2 - beforePoint._2
          var xyk_fenzi = lastPoint._1 - beforePoint._1
          if (xyk_fenmu == 0.0 || xyk_fenzi == 0.0) {
            var xyk = 0.0
            var xyb = lastPoint._2
            var xy_ = xyb
            val tuple_1 = (k(i)._1._1, k(i)._1._2, xy_)
            shengYuAll += tuple_1//赋值
          } else {
            var xyk = xyk_fenmu / xyk_fenzi
            var xyb = lastPoint._2 - (xyk * lastPoint._1)
            var xy_ = (xyk * k(i)._1._1) + xyb
            val tuple_2 = (k(i)._1._1, k(i)._1._2, xy_)
            shengYuAll += tuple_2//赋值
          }

        } else {
          val tuple_0 = (k(i)._1._1, k(i)._1._2, k(i)._1._2)
          shengYuAll += tuple_0 //赋值
        }
      }

      //写个平均值计算方法
      val avg = (arr: Array[Double]) => {
        var t = 0.0
        for (i <- arr) {
          t += i
        }
        t / arr.length
      }

      //step3 计算拟合优度R2
      var mean = avg(shengYuAll.map(x => x._2).toArray)
      var fenmu = shengYuAll.map(x => (x._3 - x._2) * (x._3 - x._2)).sum //分母总和
      var fenzi = shengYuAll.map(x => (x._2 - mean) * (x._2 - mean)).sum //分子总和
      if (fenmu == 0.0 || fenzi == 0.0) {
        R2 = 1.0
      } else {
        R2 = 1.0 - sqrt(fenmu / fenzi)
      }

      //判定拟合度是否达到要求，是则跳出while循环，否则增加选取比例
      if (R2 < targetR) {
        mark = 1
        current_sample_percentage += sample_increase
        R2 == 0.0
      } else {
        mark = 0
      }
    }
    var source_out_1 = dataPercent.map(line => (line._1, line._2)).sortBy(x => x._1).toList
    var step1ResultS_FristPiont = (source(0)._1, source(0)._2)
    var step1ResultS_LastPiont = (source(source.length - 1)._1, source(source.length - 1)._2)
    val source_out: List[(Double, Double)] = step1ResultS_FristPiont +: source_out_1 :+ step1ResultS_LastPiont //加上第一个和最后一个点
    source_out //返回值（x,y）
  }
}
