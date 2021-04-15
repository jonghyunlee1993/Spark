import org.apache.spark.sql.SaveMode

val driver = "com.mysql.jdbc.Driver"
val url = "jdbc:mysql://likebnb.org:3306/SparkCase"
val encode = "?useUnicode=true&characterEncoding=utf8"
val user = "sparkcase"
val pass = "tmvkzmzpdltm"

val recordDf = List(("김경환","산업공학과","likebnb@gmail.com","환영합니다!"))
               .toDF("name", "major", "email", "intro")

recordDf.write.format("jdbc").mode(SaveMode.Append)
              .option("url", url + encode).option("driver", driver)
              .option("user", user).option("password", pass)
              .option("dbtable", "test")
              .save()
