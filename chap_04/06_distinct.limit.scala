df.select("rentStatName").count
df.select("rentStatName").distinct.count

df.limit(1).show
df.take(1)