
/**
  * Created by jessonxu on 2017/3/22.
  */
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.HashPartitioner
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import Array._
import scala.math._

object textSimilarityVec {
    def main(args: Array[String]): Unit = {
        if(args.length != 4) {
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
        //val inputAllEvilWordsPath = args(2)         //所有恶意词集合，一列<word>
        //val inputObviousEvilWordsPath = args(3)     //明显直接恶意词集合,两列<label, word>
        val outputResultSavePath = args(2)          //比较结果存储路径
        val hashPartitionerNum = args(3).toInt      //索引分区数，最好为质数，eg:193
        //val outputInvertedIndexSavePath = args(6)   //倒排索引保存路径，用于数据倾斜时的检查
        //val onlineEvilTypePath = args(7)            //上线的恶意类型

        val inputSeedsSegmentRDD = spark.sparkContext.textFile(inputSeedsSegmentPath)
            .filter(x => x.split('\t').length == 2)
        val inputNewCorpusSegmentRDD = spark.sparkContext.textFile(inputNewCorpusSegmentPath)
            .filter(x => x.split('\t').length == 3)

        //val onlineEvilTypeRDD = spark.sparkContext.textFile(onlineEvilTypePath)

        val tmpSeedSegmentRDD = inputSeedsSegmentRDD.map{
            x =>
                val record = x.split("\t")
                record(0) + "\t" + "seed" + "\t" + record(1)
        }
        val unionSeedAndNewCorpusRDD = inputNewCorpusSegmentRDD.union(tmpSeedSegmentRDD)

        //种子文本词汇集合
        val seedsVocabularySet = inputSeedsSegmentRDD.map{ x => x.split("\t")(1).split("\t")}
            .flatMap(x => x).collect().toSet

        //过滤掉当日词频大于30000的种子词汇
        val filteredVocabularySet = inputNewCorpusSegmentRDD.map(x => x.split('\t')(2).split(' '))
            .flatMap(x => x).map(x => (x, 1)).reduceByKey(_+_).filter(x => seedsVocabularySet.contains(x._1))
            .filter(x => x._2<30000).map(x => x._1).collect().toSet

        //过滤出包含种子词的消息
        val filteredNewCorpusSegmentRDD = unionSeedAndNewCorpusRDD.map{
            x =>
                val record = x.split("\t")
                val words = record(2).split(" ")
                val wordsset = record(2).split(" ").toSet
                (record(0), record(1), record(2), words, wordsset)
        }.filter(x => x._5.intersect(filteredVocabularySet).nonEmpty).map(x => (x._1, x._2, x._3, x._4))

        val vecCorpusDF = spark.createDataFrame(filteredNewCorpusSegmentRDD).toDF("uin", "msgid", "words", "wordsArray")

        //上述过滤出来的文本为语料计算词向量，加和求平均表示单条文本的向量
        val word2vec = new Word2Vec().setNumPartitions(100).setVectorSize(300).setInputCol("wordsArray").setOutputCol("msgvec")
        val msgVecDF = word2vec.fit(vecCorpusDF).transform(vecCorpusDF)

        //msgVec.select("uin", "msgid", "words", "msgvec")

        val msgVecFeature = msgVecDF.select("uin", "msgid", "words", "msgvec").rdd.map{
            case Row(uin:String, msgid:String, words:String, msgvec:Vector) => (uin, msgid, words, msgvec.toSparse)
        }

        //以种子词为索引建立倒排索引
        val invertedVecFeature = msgVecFeature.map(x => x._3.split(' ').map(y => (y, List(x)))).flatMap(x => x)
            .filter(x => filteredVocabularySet.contains(x._1)).partitionBy(new HashPartitioner(hashPartitionerNum))
            .reduceByKey((x, y) => x:::y).filter(x => x._2.length>1)

        val resultSim = invertedVecFeature.map{
            x =>
                //val wordKey = x._1
                val simlarityResListBuffer = ListBuffer.empty[(String, String, String, Double, String, Double, String, Double, String)]
                val labeledRecord = x._2.filter(x => x._2 == "seed")
                val unlabeledRecord = x._2.filter(x => x._2 != "seed")

                for(record <- unlabeledRecord) {
                    var simResListBuffer = ListBuffer.empty[(Double, String)]
                    for(seed <- labeledRecord) {
                        val msgFeature = record._4.toDense.toArray
                        val seedFeature = seed._4.toDense.toArray
                        var numerator = 0.0
                        var denominatorA = 0.0
                        var denominatorB = 0.0
                        for(i <- range(0,300)) {
                            numerator += msgFeature(i)*seedFeature(i)
                            denominatorA += msgFeature(i)*msgFeature(i)
                            denominatorB += seedFeature(i)*seedFeature(i)
                        }
                        val similarityValue = (numerator/(sqrt(denominatorA)*sqrt(denominatorB)), seed._1)
                        simResListBuffer += similarityValue
                    }
                    val simResList = simResListBuffer.toList.sortWith(_._1>_._1)
                    var maxSim = (0.0, "")
                    var secondMaxSim = (0.0, "")
                    var thirdMaxSim = (0.0, "")
                    if(simResList.nonEmpty) {
                        maxSim = simResList.head
                    }
                    if(simResList.length>1) {
                        secondMaxSim = simResList(1)
                    }
                    if(simResList.length>2) {
                        thirdMaxSim = simResList(2)
                    }
                    val resTuple = (record._1, record._2, record._3, maxSim._1, maxSim._2, secondMaxSim._1, secondMaxSim._2, thirdMaxSim._1, thirdMaxSim._2)
                    simlarityResListBuffer += resTuple
                }
                simlarityResListBuffer.toList
        }.flatMap(x => x).map{
            x =>
                (x._1 + "\t" + x._2 +"\t" + x._3, List((x._4, x._5), (x._6, x._7), (x._8, x._9)))
        }.reduceByKey((x, y) => x:::y).map{
            x =>
                val sortedRes = x._2.sortWith(_._1>_._1)
                val maxSim = sortedRes.head
                val secSim = sortedRes(1)
                val thirdSim = sortedRes(2)
                x._1 + "\t" + maxSim._1.toString + "\t" + maxSim._2 + "\t" + secSim._1.toString + "\t" + secSim._2 + "\t" + thirdSim._1.toString + "\t" + thirdSim._2
        }

        resultSim.saveAsTextFile(outputResultSavePath)

        spark.stop()
    }
}
