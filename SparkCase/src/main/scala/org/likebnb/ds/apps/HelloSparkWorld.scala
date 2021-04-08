package org.likebnb.ds.apps

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class HelloSparkWorld {
    val LOG = HelloSparkWorld.LOG

    def helloSparkWorld() {
        LOG.info("Hello Spark World!")
    }
}

object HelloSparkWorld {
    val LOG = LoggerFactory.getLogger(classOf[HelloSparkWorld])

    def main(args: Array[String]): Unit = {
        LOG.info("The 'main' method is called and args:");
        LOG.info("  " + args(0));
        LOG.info("Now calling the method, sayHello().");
        LOG.info("  sayHello() --> "); sayHello()
    }

    def sayHello() {
        val hsw = new HelloSparkWorld()
        hsw.helloSparkWorld()
    }
}