winDf.where("rentStatId == '112'").show(200)

winDf.groupBy("rentStatid")
    .agg(
       count("useTimePerDay").alias("cnt"), 
       sum("useTimePerDay").alias("sum"), 
       min("useTimePerDay").alias("min"), 
       max("useTimePerDay").alias("max"), 
       avg("useTimePerDay").alias("avg"))
    .select("rentStatId", "cnt", "sum", "min", "max", "avg")
    .show(5)

winDf.groupBy("rentStatid")
    .agg(sum("useTimePerDay").alias("total")).sort(desc("total")).show(5)
    
val corDf = grpDf
    .withColumn("timeCumulSum", sum($"useTimePerDay").over(winSpec))
    .withColumn("distCumulSum", sum($"useDistPerDay").over(winSpec))
    .select("rentStatId", "rentDate", "useTimePerDay", "timeCumulSum", 
                                      "useDistPerDay", "distCumulSum")
    .sort("rentStatId", "rentDate")
    
corDf.groupBy("rentStatid")
    .agg(corr("useTimePerDay", "useDistPerDay").alias("corr"))
    .select("rentStatId", "corr")
    .show

corDf.groupBy("rentStatid")
    .agg(corr("useTimePerDay", "useDistPerDay").alias("corr"))
    .select("rentStatId", "corr")
    .sort(desc("corr"))
    .show(10)
    
corDf.where("rentStatId == '325'").show(200)
df.where("rentStatId == '325'").show(200)