// :paste
// ---------------------------------------------------------------------------------------
df.where("rentStatId = 112").show

// 다음 네 줄의 명령을 각각 실행하고 비교해보자 --------------
df.where("rentStatId = returnStatId").show
df.where("rentStatId" == "returnStatId").show
df.where(col("rentStatId") == col("returnStatId")).show
df.where(col("rentStatId") === col("returnStatId")).show
// -----------------------------------------------------------

df.where("useTime >= 10").show
df.where(col("useTime") >= 10).show

df.where("useDistance = 0").show
df.where("useDistance == 0").show
df.where(col("useDistance") === 0).show
// Ctrl+D
// ---------------------------------------------------------------------------------------
