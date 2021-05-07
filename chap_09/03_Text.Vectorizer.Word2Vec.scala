import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row

val textDf = spark.read.textFile(
             "/DevTools/workspaces/Spark-TDG/chap_09/spark_mllib.txt")
//           "C:/DevTools/workspaces/Spark-TDG/chap_09/spark_mllib.txt")

val regTkn = new RegexTokenizer().
                 setInputCol("value").
                 setOutputCol("reg_token").
                 setPattern("[A-Za-z]*").
                 setGaps(false).
                 setToLowercase(true)
val tknDf = regTkn.transform(textDf)

val w2v = new Word2Vec().
                 setInputCol("reg_token").
                 setOutputCol("word2vec").
                 setVectorSize(50).
                 setMinCount(3)
val model = w2v.fit(tknDf)
model.findSynonyms("scala", 5).show

val w2vDf = model.transform(tknDf)
w2vDf.collect().foreach( {
    case Row(value: String, reg_token: Seq[_], word2vec: Vector) =>
    println(s"Text: [${reg_token.mkString(",")}] => \nVector: $word2vec\n")
})
