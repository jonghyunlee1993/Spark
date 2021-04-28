// Create View: 대여소별 일자별 통계
spark.sql("""
   CREATE OR REPLACE VIEW view_stat_daily AS
      SELECT rentDate as date, rentStatId as statId, 
             count(*) as count,
             sum(useTime) as timeSum, round(avg(useTime), 2) as timeAvg,
             sum(useDistance) as distSum, round(avg(useDistance), 2) as distAvg 
        FROM AiRoBiC
       GROUP BY rentDate, rentStatId
""")

// -- 대여소별, 일자별 통계 건수, 목록 출력
spark.sql("SELECT count(*) FROM view_stat_daily").show
spark.sql("SELECT * FROM view_stat_daily order by date, statId").show(150)