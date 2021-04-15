package org.likebnb.ds.spark.common

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class Spark extends Serializable {}

object Spark {
    val masterUrl = "local[*]"
    val base = "/DevTools/workspaces/Spark-TDG"
    val javaOpt = "-XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:+UseG1GC " +
                  "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Dfile.encoding=UTF-8"

//    val base = "C:\\DevTools\\workspaces\\Spark-TDG"
//    val javaOpt = "-XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:+UseG1GC " +
//                "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=\\tmp -Dfile.encoding=UTF-8"

    var thread = 1

    def getSession(app: String, cores: Int): SparkSession = {
        this.thread = cores
        val spark = SparkSession.builder().config(getConf(app, cores)).getOrCreate()
        spark.sparkContext.setCheckpointDir(getFsBase() + "\\tmp")
        spark
    }

    def getFsBase(): String = {
        base
    }

    def getConf(app: String, cores: Int): SparkConf = {
        new SparkConf().setAppName(app).setMaster(masterUrl)
            .set("spark.eventLog.dir", getFsBase() + "\\tmp")
            .set("spark.eventLog.enabled", "true")
            .set("spark.ui.showConsoleProgress", "False")
            .set("spark.sql.crossJoin.enabled", "True")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.default.parallelism", "24")
            .set("spark.sql.shuffle.partitions", "24")
            .set("spark.driver.memory", "2g")
            .set("spark.driver.cores", "2")
            .set("spark.executor.memory", "2g")
            .set("spark.executor.cores", "6")
            .set("spark.driver.extraJavaOptions", javaOpt)
            .set("spark.executor.extraJavaOptions", javaOpt)
            .set("spark.jars", "target/SparkCase-1.0.0.jar")
    }
}
