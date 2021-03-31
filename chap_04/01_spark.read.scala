// :paste
// ---------------------------------------------------------------------------------------
val df = spark.read
              .format("csv")
              .load("AiRoBiC_2015_0519_sample.csv")
//            .load("C:/DevTools/workspaces/Spark-TDG/chap_04/AiRoBiC_2015_0519_sample.csv")

// Ctrl+D
// ---------------------------------------------------------------------------------------
df.printSchema
df.show
