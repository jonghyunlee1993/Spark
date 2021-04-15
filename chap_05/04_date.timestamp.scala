// -- DateType ---------------------------------------------------------------------------
df.select("rentDate").show
df.select(to_timestamp(col("rentDate"))).show

// 기존 spark.read.schema.option.scala 파일에서 다음의 내용 변경
// 1. DateType을 TimestampType으로 바꾸고
// 2. spark.read에 옵션 option("timestampFormat", "yyyy-MM-dd HH:mm") 추가

// -- TimestampType ----------------------------------------------------------------------
df.select("rentDate").show
df.select(to_date(col("rentDate"))).show
df.select(date_sub(col("rentDate"), 3)).show
df.select(date_add(col("rentDate"), 12)).show
df.select(datediff(date_add(col("rentDate"), 12), col("rentDate"))).show

// -- current_date(), current_timestamp() ------------------------------------------------
spark.range(1).select(current_date(), current_timestamp()).show
spark.range(1).select(datediff(current_date(), lit("2019-01-01"))).show
spark.range(1).select(months_between(current_date(), lit("2019-01-01"))).show
