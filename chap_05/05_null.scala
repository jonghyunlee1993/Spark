// -- 대여소의 이름이 없는 경우 아이디를 반환 --------------------
df.select(coalesce(col("rentStatName"), col("rentStatId"))).show

// -- useDistance 컬럼에 1.2를 곱함 ------------------------------
df.select("useDistance * 1.2").show
df.selectExpr("useDistance * 1.2").show
df.selectExpr("ifnull(useDistance, 0) * 1.2").show

// -- drop의 any, all 옵션 비교 ----------------------------------
df.na.drop.show
df.na.drop("any").show
df.na.drop("all").show

// -- fill을 사용한 결측치 처리 ----------------------------------
df.na.fill(0:Long).show  // Long 타입만 null값을 '0'으로 채움
df.na.fill(Map("rentStatId" -> "000", "rentStatName" -> "Unknown")).show
df.na.replace("rentStatName", Map("-" -> "Unknown")).show