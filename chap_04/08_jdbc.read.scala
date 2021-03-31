val driver = "com.mysql.jdbc.Driver"
val url = "jdbc:mysql://likebnb.org:3306/SparkCase"
val encode = "?useUnicode=true&characterEncoding=utf8"
val user = "sparkcase"
val pass = "tmvkzmzpdltm"

val jdbcDf = spark.read.format("jdbc")
                  .option("url", url + encode).option("driver", driver)
                  .option("user", user).option("password", pass)
                  .option("dbtable", "test")
                  .load()
