package org.likebnb.ds.spark

import org.likebnb.ds.spark.common.Spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col }

import org.junit.After
import org.junit.AfterClass
import org.junit.Assert
import org.junit.Before
import org.junit.BeforeClass
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.junit.Test

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object WordCount2Test {
    val LOG = LoggerFactory.getLogger(classOf[WordCount2Test])
    val wc2: WordCount2 = new WordCount2()
    var words: DataFrame = null
    var counts: DataFrame = null
    var sorted: DataFrame = null

    @BeforeClass def init() {
        LOG.info("========================================")
        LOG.info("This method executes before all tests.")
        LOG.info("========================================")
    }

    @AfterClass def clean() {
        LOG.info("========================================")
        LOG.info("This method executes after all tests.")
        LOG.info("========================================")
    }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class WordCount2Test extends Serializable {

    val LOG = WordCount2Test.LOG

    @Test def test01() {
        LOG.info("테스트케이스-01 : 파일경로 지정 후 확인")
        // val filepath = Spark.getFsBase() + "/chap_07/politic_bluehouse_5.txt"
        val filepath = Spark.getFsBase() + "/chap_07/my_news.txt"
        WordCount2Test.wc2.setFilepath(filepath)
        Assert.assertEquals(WordCount2Test.wc2.getFilepath(), filepath)
        LOG.info("파일경로: {}", WordCount2Test.wc2.getFilepath())
    }

    @Test def test02() {
        LOG.info("테스트케이스-02 : 형태소 분석 후 텀 개수 확인")
        WordCount2Test.words = WordCount2Test.wc2.makeWords()
        Assert.assertEquals(WordCount2Test.words.count(), 151)
        LOG.info("형태소분석 결과 텀의 개수: {}", WordCount2Test.words.count())
    }

    @Test def test03() {
        LOG.info("테스트케이스-03 : 텀빈도 계산 및 확인")
        WordCount2Test.counts = WordCount2Test.wc2.makeCounts()
        WordCount2Test.counts.show(3)
        Assert.assertEquals(WordCount2Test.counts.first.getString(0), "동안")
        LOG.info("텀빈도 계산 결과 첫번째 Row: {}", WordCount2Test.counts.first)
    }

    @Test def test04() {
        LOG.info("테스트케이스-04 : 텀빈도를 기준으로 내림차순 정렬")
        WordCount2Test.sorted = WordCount2Test.wc2.sortCounts("count")
        WordCount2Test.sorted.show(3)
        Assert.assertEquals(WordCount2Test.sorted.first.getString(0), "약사")
        LOG.info("정렬 후 첫번째 Row: {}", WordCount2Test.sorted.first)
    }

    @Before def before() {
        LOG.info("----------------------------------------------------------------")
    }
}
