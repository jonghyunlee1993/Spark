// :paste spark.read.schema.option.timestamp.scala
// :paste null.cleansing.scala <-- MS949

// -- 데이터셋을 파악하기 위해 제일 먼저 확인하는 통계량 --
fillDf.select(count("useTime"), min("useTime"), max("useTime"), avg("useTime")).show
fillDf.select(first("useTime"), last("useTime")).show

// -- 고유한 Row에 대해서만 계산 --------------------------
fillDf.select(sumDistinct("useTime"), sum("useTime")).show
fillDf.select(countDistinct("rentStatId"), count("rentStatId")).show

// -- 최대추정오류율(maximum estimation error) ------------
fillDf.select(approx_count_distinct("rentStatId", 0.1)).show
fillDf.select(approx_count_distinct("rentStatId", 0.9)).show
fillDf.select(approx_count_distinct("rentStatId", 0.3)).show
fillDf.select(approx_count_distinct("rentStatId", 0.01)).show

// -- deprecated function ---------------------------------
fillDf.select(approxCountDistinct("rentStatId", 0.1)).show

// -- 분산과 표준편차 -------------------------------------
fillDf.select(variance("useTime"), var_pop("useTime"), var_samp("useTime")).show
fillDf.select(stddev("useTime"), stddev_pop("useTime"), stddev_samp("useTime")).show

// -- 비대칭도와 첨도 -------------------------------------
fillDf.select(skewness("useTime"), kurtosis("useTime")).show
fillDf.select(skewness("useDistance"), kurtosis("useDistance")).show

// -- 공분산과 상관관계 -----------------------------------
fillDf.select(covar_pop("useTime", "useDistance")).show
fillDf.select(covar_samp("useTime", "useDistance")).show
fillDf.select(corr("useTime", "useDistance")).show

// -- collect_set  ----------------------------------------
fillDf.agg(collect_set("rentStatId")).show
fillDf.agg(collect_set("rentStatId")).collect.foreach(println)
fillDf.agg(collect_set("rentStatId")).first.getList(0)
      .toArray.foreach(println)

// -- collect_set with explode, sort ----------------------
fillDf.agg(collect_set("rentStatId").alias("values"))
      .select(explode(col("values")).alias("explode"))
      .sort("explode").show

// -- collect_list ----------------------------------------
fillDf.agg(collect_list("rentStatId")).show
fillDf.agg(collect_list("rentStatId")).collect.foreach(println)
fillDf.agg(collect_list("rentStatId")).first.getList(0)
      .toArray.foreach(println)

// -- collect_list with explode, sort ---------------------
fillDf.agg(collect_list("rentStatId").alias("values"))
      .select(explode(col("values")).alias("explode"))
      .sort("explode").show
