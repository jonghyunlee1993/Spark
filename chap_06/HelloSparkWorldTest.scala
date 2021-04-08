package org.likebnb.ds.apps

import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object HelloSparkWorldTest {
    val LOG = LoggerFactory.getLogger(classOf[HelloSparkWorld])

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

class HelloSparkWorldTest {
    val LOG = HelloSparkWorldTest.LOG
    val hsw: HelloSparkWorld = new HelloSparkWorld()

    @Before def before() {
        LOG.info("========================================")
        LOG.info("execute testcase  ")
        LOG.info("----------------------------------------")
    }

    @After def after() {
        LOG.info("========================================")
    }

    @Test def test1() {
        hsw.helloSparkWorld()
    }

    @Test def test2() {
        val greetings = new Array[String](1)
        greetings(0) = "Hi, I'm barnabas, welcome to Spark World!"
        HelloSparkWorld.main(greetings)
    }
}