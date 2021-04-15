// :paste
// ---------------------------------------------------------------------------------------
df.count

// 다음 각 줄의 상관계수를 보고 설명해보자  ------------------
df.stat.corr("useTime", "useDistance")
df.select(corr("useTime", "useDistance")).show
df.select(corr("rentStatId", "useTime")).show
df.select(corr("rentStatId", "useDistance")).show
df.select(corr("returnStatId", "useTime")).show
df.select(corr("returnStatId", "useDistance")).show
// -----------------------------------------------------------
df.stat.crosstab("useTime", "useDistance").show

val numDf = df.select("useTime", "useDistance")
df.describe().show
numDf.describe().show

numDf.describe().select(col("summary"), bround(col("useTime"),4)).show
// Ctrl+D
// ---------------------------------------------------------------------------------------
