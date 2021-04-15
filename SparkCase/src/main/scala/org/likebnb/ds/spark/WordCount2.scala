package org.likebnb.ds.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col}

import org.bitbucket.eunjeon.seunjeon.Analyzer

import org.likebnb.ds.spark.common.Spark
import org.likebnb.ds.spark.analyzer.KoreanAnalyzer

class WordCount2 extends Serializable {
    var filepath: String = ""
    var words: DataFrame = null
    var counts: DataFrame = null
    var sorted: DataFrame = null

    def setFilepath(filepath: String) {
        this.filepath = filepath
    }

    def getFilepath(): String = {
        this.filepath
    }

    def makeWords(): DataFrame = {
        this.words = WordCount2.getWords(this.filepath)
        words
    }

    def makeCounts(): DataFrame = {
        this.counts = WordCount2.countByKey(this.words, "term")
        counts
    }

    def sortCounts(key: String): DataFrame = {
        this.sorted = WordCount2.sortByKey(this.counts, key, true)
        sorted
    }
}

object WordCount2 {
    val spark = Spark.getSession("", 4)
    import spark.implicits._
//    var file = Spark.getFsBase() + "\\chap_07\\politic_bluehouse_5.txt"
    // var file = Spark.getFsBase() + "/chap_07/politic_bluehouse_5.txt"
    var file = Spark.getFsBase() + "/chap_07/my_news.txt"

    def main(args: Array[String]) {
        if (args.length > 0) {
            file = args(0)
        }

        val words = getWords(file)
        val counts = countByKey(words, "term")
        val sorted = sortByKey(counts, "count", true)
//        sorted.show(30)
        spark.stop()
    }

    def getWords(filepath: String): DataFrame = {
        val text = spark.read.textFile(filepath)
        val terms = text.flatMap(line => KoreanAnalyzer.analyze(line))
        val words = KoreanAnalyzer.filterPos(terms, "N")
        words
    }

    def countByKey(df: DataFrame, colName: String): DataFrame = {
        val counted = df.groupBy(col(colName)).count
//        counted.printSchema
//        counted.show(5)
        counted
    }

    def sortByKey(df: DataFrame, key: String, reverse: Boolean): DataFrame = {
        var sorted: DataFrame = null
        if (reverse) {
            sorted = df.sort(col(key).desc)
        } else {
            sorted = df.sort(col(key))
        }
//        sorted.printSchema
//        sorted.show(5)
        sorted
    }
}
