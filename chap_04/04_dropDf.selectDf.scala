val dropDf = df.drop("bicNumber", "rentDate", "rentStatid", 
                     "rentStatName", "rentParkId", 
                     "returnDate", "returnStatId", 
                     "returnStatName", "returnParkId")

val selectDf = df.select("useTime", "useDistance")

val addDf = df.withColumn("bicNum", regexp_replace(df("bicNumber"), "SPB-", ""))
              .select("bicNumber", "bicNum")
              
val castDf = addDf.withColumn("intBicNum", col("bicNum").cast("Int"))