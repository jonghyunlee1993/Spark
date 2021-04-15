// -- rentDate, rentStatId 컬럼들을 묶어 만든 compStruct ---------------------------------
val airoStruct = fillDf.withColumn("compStruct", struct(col("rentDate"), col("rentStatId")))
airoStruct.select("compStruct").show
airoStruct.select(col("compStruct").getField("rentStatId")).show

// -- rentStatName 컬럼을 split하여 만든 compArray ---------------------------------------
val airoArray = fillDf.withColumn("compArray", split(col("rentStatName"), " "))
airoArray.select("compArray").show
airoArray.select(size(col("compArray"))).show
airoArray.select(array_contains(col("compArray"), "앞")).show
airoArray.select(explode(col("compArray"))).show

// -- rentStatId -> rentStatName 쌍으로 선언한 compMap -----------------------------------
val airoMap = fillDf.withColumn("compMap", map(col("rentStatId"), col("rentStatName")))
airoMap.selectExpr("compMap[rentStatId]").show
airoMap.selectExpr("explode(compMap)").show
airoMap.select(explode(col("compMap"))).show
