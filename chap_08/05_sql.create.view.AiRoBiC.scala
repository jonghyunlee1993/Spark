// -- Create View: 대여소 목록 
spark.sql("""
   CREATE OR REPLACE VIEW view_station AS
      SELECT DISTINCT rentStatId as statId, rentStatName as statName 
        FROM AiRoBiC
""")

// -- 대여소가 몇 개인지 확인, 목록 출력
spark.sql("SELECT count(*) FROM view_station").show
spark.sql("SELECT * FROM view_station order by statId").show(150)

// Create View: 대여소별 일자별 통계
spark.sql("""
   CREATE OR REPLACE VIEW view_stat_daily AS
      SELECT rentDate as date, rentStatId as statId, 
             count(*) as count,
             sum(useTime) as timeSum, avg(useTime) as timeAvg,
             sum(useDistance) as distSum, avg(useDistance) as distAvg 
        FROM AiRoBiC
       GROUP BY rentDate, rentStatId
""")

// -- 대여소별, 일자별 통계 건수, 목록 출력
spark.sql("SELECT count(*) FROM view_stat_daily").show
spark.sql("SELECT * FROM view_stat_daily order by date, statId").show(150)
