// :paste
// ---------------------------------------------------------------------------------------
df.select(initcap(col("bicNumber"))).show
df.select(lower(col("bicNumber"))).show
df.select(upper(lower(col("bicNumber")))).show

df.select(lpad(lit("SPARK"), 6, " ").as("lpad")).show
df.select(lpad(lit("SPARK"), 4, " ").as("lpad")).show
df.select(rpad(lit("SPARK"), 10, " ").as("rpad")).show

df.select(trim(lit("  SPARK  ")).as("trim")).show
df.select(ltrim(lit("  SPARK  ")).as("ltrim")).show
df.select(rtrim(lit("  SPARK  ")).as("rtrim")).show

df.select(regexp_replace(col("bicNumber"), "-", "_").as("rep")).show
df.where(col("rentStatName").contains("여의도")).show
df.select(translate(col("rentStatName"), "124", "일이사").as("trs")).show
// Ctrl+D
// ---------------------------------------------------------------------------------------
