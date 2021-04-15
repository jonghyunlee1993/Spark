// -- read from text file ---------------------
val textDf = spark.read
                  .textFile("/DevTools/workspaces/Spark-TDG/chap_04/spark_overview.txt")
//                .textFile("C:/DevTools/workspaces/Spark-TDG/chap_04/spark_overview.txt")

// -- tokenization ----------------------------
val words = textDf.flatMap(line => line.split(" "))

// -- group & count ---------------------------
val counts = words.groupByKey(_.toLowerCase).count()

// -- retrieve  WordCount ---------------------
counts.printSchema
counts.count
counts.sort($"value").show
counts.sort($"count(1)".desc).show
