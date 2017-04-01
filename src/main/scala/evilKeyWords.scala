import org.apache.spark.sql.SparkSession
import scala.math.log10

/**
  * Created by jessonxu on 2017/2/17.
  */
object evilKeyWords {
    def main(args: Array[String]): Unit = {
        if (args.length != 3) {
            println {
                "Usage: need 3 arguments, input " + args.length.toString + " arguments"
            }
            sys.exit(-1)
        }

        @transient lazy val spark = SparkSession
            .builder
            .appName("evilKeyWords")
            .getOrCreate()

        val seedFilePath = args(0)
        val corpusFilePath = args(1)
        val evilWordsSavePath = args(2)

        val seedFileRDD =  spark.sparkContext.textFile(seedFilePath).filter(x => x.split('\t').length ==2)
        val corpusFileRDD =  spark.sparkContext.textFile(corpusFilePath).filter(x => x.split('\t').length ==3)

        val seedDocWordSet = seedFileRDD.map(x => x.split('\t')(1).split(' ')).flatMap(x => x).collect().toSet
        val corpusDocTFMap = corpusFileRDD.map(x => x.split('\t')(2).split(' ')).flatMap(x => x)
            .map(x => (x, 1)).filter( x => seedDocWordSet.contains(x._1)).reduceByKey(_+_).collect().toMap
        val newMessageNum = corpusFileRDD.count()

        val evilWordsImportance = seedFileRDD.map{
            x =>
                val record = x.split('\t')
                val words = record(1).split(' ').map(x => record(0) + '#' + x)
                words
        }.flatMap(x => x).map(x => (x, 1)).reduceByKey(_+_).map{
            x =>
                val word = x._1.split('#')
                (word(0), word(1)+'#'+x._2.toString)
        }.reduceByKey((x, y) => x + '|' + y).map{
            x =>
                val maxTF = x._2.split('|').map(x => x.split('#')(1).toInt).toList.sortWith(_>_).head
                val category = x._2.split('|').map{
                    y =>
                        val record = y.split('#')
                        val word = record(0)
                        val tf1 = record(1)
                        var tf2 = 0
                        if(corpusDocTFMap.contains(word)) {
                            tf2 = corpusDocTFMap(word)
                        }
                        val weight = (0.5+0.5*(tf1.toDouble/maxTF))*log10((newMessageNum+1)/(tf2+1))
                        x._1+'\t'+word+'\t'+tf1.toString+'\t'+tf2.toString+'\t'+weight.formatted("%.4f").toString
                }
                category
        }.flatMap(x => x)

        evilWordsImportance.saveAsTextFile(evilWordsSavePath)

        spark.stop()
    }
}
