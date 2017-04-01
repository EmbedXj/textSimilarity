import org.apache.spark.sql.SparkSession

/**
  * Created by jessonxu on 2017/2/17.
  */
object filterSpecialChar {
    def main(args: Array[String]): Unit = {
        if (args.length != 2) {
            println {
                "Usage: need 2 arguments, input " + args.length.toString + " arguments"
            }
            sys.exit(-1)
        }

        @transient lazy val spark = SparkSession
            .builder
            .appName("filterSpecialChar")
            .getOrCreate()

        val inputFilePath = args(0)
        val outputFilePath = args(1)

        val inputCorpusRDD = spark.sparkContext.textFile(inputFilePath).filter(x => x.split('\t').length == 3)

        val regx = """[0-9 ,\u4e00-\u9fa5]+""".r

        val outputCorpusRDD = inputCorpusRDD.map{
            x =>
                val record = x.split('\t')
                val filterEmojiExpression = record(2).split("""\[(微笑|撇嘴|色|发呆|得意|流泪|害羞|闭嘴|睡|大哭|尴尬|发怒|调皮|呲牙|惊讶|难过|囧|抓狂|吐|偷笑|愉快|白眼|傲慢|困|惊恐|流汗|憨笑|悠闲|奋斗|咒骂|疑问|嘘|晕|衰|骷髅|敲打|再见|擦汗|抠鼻|鼓掌|坏笑|左哼哼|右哼哼|哈欠|鄙视|委屈|快哭了|阴险|亲亲|可怜|菜刀|西瓜|啤酒|咖啡|猪头|玫瑰|凋谢|嘴唇|爱心|心碎|蛋糕|炸弹|便便|月亮|太阳|拥抱|强|弱|握手|胜利|抱拳|勾引|拳头|OK|跳跳|发抖|怄火|转圈|捂脸|嘿哈|奸笑|机智|皱眉|耶|红包|福|鸡|闪电)\]""").mkString
                val filterMessage = regx.findAllMatchIn(filterEmojiExpression).mkString
                record(0) + '\t' + record(1) + '\t' + filterMessage
        }
        outputCorpusRDD.saveAsTextFile(outputFilePath)

        spark.stop()
    }
}
