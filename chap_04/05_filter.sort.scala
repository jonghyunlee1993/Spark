df.filter(col("useTime") > 10).show     // filter, where의 동작은 동일함
df.where("useTime > 10").show

df.sort("rentStatId").show              // 암묵적인 오름차순
df.sort(desc("rentStatId")).show        // 명시적인 내림차순

df.orderBy("rentStatId").show           // sort와 orderBy 동작은 동일함
df.orderBy(desc("rentStatId")).show
