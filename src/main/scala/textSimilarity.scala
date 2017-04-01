
/**
  * Created by jessonxu on 2017/2/16.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import scala.collection.mutable.ListBuffer

import scala.math.log10

object textSimilarity {
    def main(args: Array[String]): Unit = {
        if(args.length != 8) {
            println {
                "Usage: need 8 arguments, input " + args.length.toString + " arguments"
            }
            sys.exit(-1)
        }

        @transient lazy val spark = SparkSession
            .builder
            .appName("TextSimilarity")
            .getOrCreate()

        val inputSeedsSegmentPath = args(0)         //分词后的种子文本，带标签<label, words>
        val inputNewCorpusSegmentPath = args(1)     //分词后的待比较文本，三列<uin, messageID， message>
        val inputAllEvilWordsPath = args(2)         //所有恶意词集合，一列<word>
        val inputObviousEvilWordsPath = args(3)     //明显直接恶意词集合,两列<label, word>
        val outputResultSavePath = args(4)          //比较结果存储路径
        val hashPartitionerNum = args(5).toInt      //索引分区数，最好为质数，eg:193
        val outputInvertedIndexSavePath = args(6)   //倒排索引保存路径，用于数据倾斜时的检查
        val onlineEvilTypePath = args(7)            //上线的恶意类型

        //val typeSet = Set("LIVEPORN", "LIVEPORN_NEG", "FLG", "FLG_NEG", "BANNEDBOOK", "BANNEDBOOK_NEG", "CHILDPORN", "CHILDPORN_NEG", "CLOUDDISKPORN","CLOUDDISKPORN_NEG")

        val inputSeedsSegmentRDD = spark.sparkContext.textFile(inputSeedsSegmentPath)
            .filter(x => x.split('\t').length == 2)
        val inputNewCorpusSegmentRDD = spark.sparkContext.textFile(inputNewCorpusSegmentPath)
            .filter(x => x.split('\t').length == 3)
        val inputAllEvilWordsRDD = spark.sparkContext.textFile(inputAllEvilWordsPath)
        val inputObviousEvilWordsRDD = spark.sparkContext.textFile(inputObviousEvilWordsPath)
            .filter(x => x.split('\t').length == 2)

        val onlineEvilTypeRDD = spark.sparkContext.textFile(onlineEvilTypePath)
        val typeSet = onlineEvilTypeRDD.collect().toSet

        val allEvilWords = inputAllEvilWordsRDD.collect().toSet
        val obviousEvilWords = inputObviousEvilWordsRDD.map(x => x.split('\t')(1)).collect().toSeq
        val obviousEvilWordsMap = inputObviousEvilWordsRDD.map(x => (x.split('\t')(1), x.split('\t')(0))).collect().toMap

        val tmpSeedSegmentRDD = inputSeedsSegmentRDD.map{
            x =>
                val record = x.split("\t")
                record(0) + "\t" + "" + "\t" + record(1)
        }
        val unionSeedAndNewCorpusRDD = inputNewCorpusSegmentRDD.union(tmpSeedSegmentRDD)

        //过滤出包含恶意词的消息
        val filteredNewCorpusSegmentRDD = unionSeedAndNewCorpusRDD.map{
            x =>
                val record = x.split("\t")
                val words = record(2).split(" ").toSet
                (record(0), words, x, record(2))
        }.filter(x => x._2.intersect(allEvilWords).nonEmpty)

        //过滤出恶意词所在索引
        val invertedIndexRDD = filteredNewCorpusSegmentRDD.map(x => x._4.split(" ").map(y => (y, x._3))).flatMap(x => x)
            .filter(x => allEvilWords.contains(x._1)).partitionBy(new HashPartitioner(hashPartitionerNum))
            .reduceByKey((x, y) => x + "|" + y).filter(x => x._2.split("|").length > 1)

        //计算恶意词的权重
        val seedCorpusTFRDD = inputSeedsSegmentRDD.map(x => x.split('\t')(1).split(' '))
            .flatMap(x => x).map(x => (x, 1)).reduceByKey(_+_).filter(x => allEvilWords.contains(x._1))
        val newCorpusTFRDD = inputNewCorpusSegmentRDD.map(x => x.split('\t')(2).split(' '))
            .flatMap(x => x).map(x => (x, 1)).reduceByKey(_+_).filter(x => allEvilWords.contains(x._1))

        val maxSeedWordTF = seedCorpusTFRDD.map(x => x._2).collect().toList.sortWith(_>_).head
        val newMessageNum = inputNewCorpusSegmentRDD.count()

        val evilWordsMinWeight = seedCorpusTFRDD.join(newCorpusTFRDD)
            .map(x => (x._1, x._2._1, x._2._2))
            .map{
                x => 0.6 + 0.4*(x._2.toDouble/maxSeedWordTF.toDouble)*log10((newMessageNum + 1)/(x._3 + 1))
            }.collect().toList.sortWith(_<_).head
        val evilWordsWeightMap = seedCorpusTFRDD.join(newCorpusTFRDD)
            .map(x => (x._1, x._2._1, x._2._2))
            .map{
                x => (x._1, 0.6 + 0.4*(x._2.toDouble/maxSeedWordTF.toDouble)*log10((newMessageNum + 1)/(x._3 + 1)))
            }.collect().toMap

        //计算待比较文本与种子文本的最大相似度
        val resultRDD = invertedIndexRDD.map{
            x =>
                val wordKey = x._1
                var similarityResListBuffer = ListBuffer.empty[(String, (String, String, String, Double, Int, String, Double, Int, String, Double, Int))]
                val messageList = x._2.split('|').map{x => x.split("\t")}

                var hitKeyword = ("", "")
                if(obviousEvilWords.contains(wordKey)) {
                    hitKeyword = (wordKey, obviousEvilWordsMap(wordKey))
                }
                val labeledRecord = messageList.filter(x => typeSet.contains(x(0)))
                    .map(x => (x(0), x(1), x(2), x(2).split(' ').toSet))
                val unlabeledRecord = messageList.filter(x => !typeSet.contains(x(0)))
                    .map(x => (x(0), x(1), x(2), x(2).split(' ').toSet))
                for(record <- unlabeledRecord) {
                    var simResListBuffer = ListBuffer.empty[(Double, Int, String)]
                    for(seed <- labeledRecord) {
                        val samewords = record._4 & seed._4
                        val sameEvilWords = samewords & evilWordsWeightMap.keySet
                        val sameCommonWords = samewords -- evilWordsWeightMap.keySet
                        val allCommonWords = record._4.union(seed._4) -- evilWordsWeightMap.keySet
                        val allEvilWords = record._4.union(seed._4) & evilWordsWeightMap.keySet
                        var numerator = 0.0
                        var denominator = 0.0
                        for(word <- sameEvilWords) {
                            numerator += evilWordsWeightMap(word)*(1/evilWordsMinWeight)
                        }
                        for(word <- allEvilWords) {
                            denominator += evilWordsWeightMap(word)*(1/evilWordsMinWeight)
                        }
                        numerator += sameCommonWords.size
                        denominator += allCommonWords.size
                        val similarity = (numerator/denominator, samewords.size, seed._1)
                        simResListBuffer += similarity
                    }
                    val simResList = simResListBuffer.toList.sortWith(_._1>_._1)
                    var maxSim = (0.0, 0, "")
                    var secondMaxSim = (0.0, 0, "")
                    var thirdMaxSim = (0.0, 0, "")
                    if(simResList.nonEmpty) {
                        maxSim = simResList.head
                    }
                    if(simResList.length>1) {
                        secondMaxSim = simResList(1)
                    }
                    if(simResList.length>2) {
                        thirdMaxSim = simResList(2)
                    }
                    val resTuple = (record._1 + "\t" + record._2 + "\t" + record._3, (hitKeyword._1, hitKeyword._2, maxSim._3, maxSim._1, maxSim._2, secondMaxSim._3, secondMaxSim._1, secondMaxSim._2, thirdMaxSim._3, thirdMaxSim._1, thirdMaxSim._2))
                    similarityResListBuffer += resTuple
                }
                similarityResListBuffer.toList
        }.flatMap(x => x).map{
            x =>
            x._1 + "\t" + x._2._1 + "\t" + x._2._2 + "\t" + x._2._3 + "\t" + x._2._4 + "\t" + x._2._5 + "\t" + x._2._6 + "\t" + x._2._7 + "\t" + x._2._8 + "\t" + x._2._9 + "\t" + x._2._10 + "\t" + x._2._11
        }.distinct()

        //spark.sparkContext.parallelize(evilWordsWeightMap.toSeq).saveAsTextFile("/dws/credit/textsimilarity/words_tf_weight")
        //newCorpusTFRDD.saveAsTextFile("/dws/credit/textsimilarity/words_tf_all")
        //invertedIndexRDD.saveAsTextFile(outputInvertedIndexSavePath)
        resultRDD.saveAsTextFile(outputResultSavePath)

        spark.stop()
    }
}
