// :paste
// -----------------------------------------------------------------------
import org.bitbucket.eunjeon.seunjeon._

//val text = spark.read.textFile(Spark.getFsBase() + "\\chap_07\\politic_bluehouse_5.txt")
val text = spark.read.textFile(Spark.getFsBase() + "/chap_07/politic_bluehouse_5.txt")
val words = text.flatMap(line => analyze(line)).toDF
words.printSchema

val wordCount = words.groupBy("Value").count()
wordCount.cache
wordCount.printSchema
wordCount.sort(col("Value")).show
wordCount.sort(col("count").desc, col("Value")).show

wordCount.collect.foreach(println)

def analyze(line: String) = {
    val terms = Analyzer.parse(line)
        .flatMap(_.deCompound())
        .filter(_.morpheme.poses.apply(0)
                .toString()
                .equals("N"))

    terms.map(node => (node.morpheme.surface))
}
// Ctrl+D
// -----------------------------------------------------------------------


//val text = spark.read.textFile(Spark.getFsBase() + "\\chap_07\\politic_bluehouse_5.txt")
val text = spark.read.textFile(Spark.getFsBase() + "/chap_07/politic_bluehouse_5.txt")
val word = text.flatMap(row => KoreanAnalyzer.analyze(row))
val word = text.collect.flatMap(line => KoreanAnalyzer.analyze(line))
                    .map(word => (word, 1))
                    .groupBy(_._1)
                    .mapValues(_.size)
                    .toArray
