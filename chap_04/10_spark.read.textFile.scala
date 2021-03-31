// :paste
// ---------------------------------------------------------------------------------------
val textDf = spark.read
                  .textFile("spark_overview.txt").toDF
//                .textFile("C:/DevTools/workspaces/Spark-TDG/chap_04/spark_overview.txt").toDF

// Ctrl+D
// ---------------------------------------------------------------------------------------
textDf.printSchema
textDf.show
textDf.collect.foreach(println)
