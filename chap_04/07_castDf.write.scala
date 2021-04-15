// :paste
// ---------------------------------------------------------------------------------------
castDf.write
      .format("csv")
      .mode("overwrite")
      .option("header", true)
      .save("AiRoBiC_2015_0519_CAST")
//    .save("C:/DevTools/workspaces/Spark-TDG/chap_04/AiRoBiC_2015_0519_CAST")

// Ctrl+D
// ---------------------------------------------------------------------------------------
df.printSchema
df.show
