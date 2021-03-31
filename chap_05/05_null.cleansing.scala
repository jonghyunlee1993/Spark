// -- rendStatId, rentStatName 컬럼이 모두 null 인경우 drop ------------------------------
val dropDf = df.na.drop("all", Seq("rentStatId", "rentStatName"))

// -- rentStatName 컬럼이 "-" 경우도 null로 통일 -----------------------------------------
val replDf = dropDf.na.replace("rentStatName", Map("-" -> null))

// -- useDistane 컬럼이 null인 경우 0으로 대체 -------------------------------------------
val fillDf = replDf.na.fill(0:Long)

// -- 다음과 같이 한 번에 처리할 수도 있다 -----------------------------------------------
// val fillDf = df.na.drop("all", Seq("rentStatId", "rentStatName"))
//                .na.replace("rentStatName", Map("-" -> null))
//                .na.fill(0:Long)
// ---------------------------------------------------------------------------------------
 
// -- rendStatId 컬럼을 기준으로 rentStatName이 null인 컬럼을 정리  ----------------------
fillDf.select("rentStatId").where("rentStatName is null").show
val statIds = fillDf.select("rentStatId", "rentStatName")
                    .where("rentStatName is not null")
                    .distinct

// -- 메쏘드 선언 ------------------------------------------------------------------------
// import org.apache.spark.sql.types.StringType
def findStatName(id: String): String = {
    var cond = "rentStatid = '".concat(id).concat("'")
    val name = statIds.select("rentStatName").where(cond)
    name.first.getString(0)
}

// -- cache: 앞으로 자주 쓸 것이므로 메모리에 저장해두자 ---------------------------------
fillDf.show
statIds.show
fillDf.cache
statIds.cache
