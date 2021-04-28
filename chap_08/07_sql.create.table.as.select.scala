// Create Table: 대여소별 일자별 통계
spark.sql("""
   CREATE TABLE dm_stat_daily AS
      SELECT * 
        FROM view_stat_daily
""")

scala> spark.sql("show tables").show
+--------+---------------+-----------+
|database|      tableName|isTemporary|
+--------+---------------+-----------+
| default|        airobic|      false|
| default|  dm_stat_daily|      false|
| default|view_stat_daily|      false|
| default|   view_station|      false|
+--------+---------------+-----------+
