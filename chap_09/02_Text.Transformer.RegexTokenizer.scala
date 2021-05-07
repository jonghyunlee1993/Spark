import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover

val textDf = spark.read.textFile(
             "/DevTools/workspaces/Spark-TDG/chap_04/spark_overview.txt")
//           "C:/DevTools/workspaces/Spark-TDG/chap_04/spark_overview.txt")

val regTkn = new RegexTokenizer().
                 setInputCol("value").
                 setOutputCol("reg_token").
                 setPattern("[A-Za-z0-9]*").
                 setGaps(false).
                 setToLowercase(true)
val tknDf = regTkn.transform(textDf)

val engSWs = StopWordsRemover.loadDefaultStopWords("english")
val stopWd = new StopWordsRemover().
                 setStopWords(engSWs).
                 setInputCol("reg_token").
                 setOutputCol("stop_token")
val bowDf = stopWd.transform(tknDf)

val counts = bowDf.withColumn("words", explode(bowDf.col("stop_token"))).
                 groupBy("words").count

counts.printSchema
counts.count
counts.sort($"words").show
counts.sort($"count".desc).show
