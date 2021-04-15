grpDf.rollup("rentStatId", "rentDate").agg(sum("useTimePerDay").alias("tot"))
   .select("rentStatId", "rentDate", "tot")
   .orderBy("rentStatId","rentDate").show

grpDf.cube("rentStatId", "rentDate").agg(sum("useTimePerDay").alias("tot"))
   .select("rentStatId", "rentDate", "tot")
   .orderBy("rentStatId","rentDate").show
   
