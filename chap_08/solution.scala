spark.sql("""
  CREATE TABLE AiRoBiC (
     bicNumber      String    COMMENT '자전거번호',
     rentDate       Timestamp COMMENT '대여일자',
     rentStatId     String    COMMENT '대여소번호',
     rentStatName   String    COMMENT '대여소이름',
     rentParkId     String    COMMENT '거치대번호',
     returnDate     Timestamp COMMENT '반납일자',
     returnStatId   String    COMMENT '반납대여소번호',
     returnStatName String    COMMENT '반납대여소이름',
     returnParkId   String    COMMENT '반납거치대번호',
     useTime        Long      COMMENT '사용시간(분)',
     useDistance    Float     COMMENT '사용거리(미터)')
  USING csv OPTIONS (
     header true,
     path '../data/seoul_bike.csv')
""")

spark.sql("DESC AIRoBiC").show

spark.sql("""
  CREATE OR REPLACE VIEW view_stat_date AS
    SELECT DATE(rentDate) AS date, 
          COUNT(*) AS useCount,
          SUM(useTime) AS timeSum, 
          ROUND(AVG(useTime), 2) AS timeAvg,
          ROUND(SUM(useDistance), 2) AS distSum, 
          ROUND(AVG(useDistance), 2) AS distAvg 
    FROM AiRoBiC  
    GROUP BY date
    ORDER BY date
""")

spark.sql("SELECT * FROM view_stat_date").show(31)

spark.sql("""
  CREATE OR REPLACE VIEW view_stat_week AS
    SELECT DAYOFWEEK(rentDate) AS dayNumb, 
          COUNT(*) AS useCount,
          SUM(useTime) AS timeSum, 
          ROUND(AVG(useTime), 2) AS timeAvg,
          ROUND(SUM(useDistance), 2) AS distSum, 
          ROUND(AVG(useDistance), 2) AS distAvg 
    FROM AiRoBiC  
    GROUP BY dayNumb
    ORDER BY dayNumb
""")

spark.sql("SELECT * FROM view_stat_week").show(7)

spark.sql("""
  CREATE OR REPLACE VIEW view_stat_hour AS
    SELECT HOUR(rentDate) AS hour, 
          COUNT(*) AS useCount,
          SUM(useTime) AS timeSum, 
          ROUND(AVG(useTime), 2) AS timeAvg,
          ROUND(SUM(useDistance), 2) AS distSum, 
          ROUND(AVG(useDistance), 2) AS distAvg 
    FROM AiRoBiC  
    GROUP BY hour
    ORDER BY hour
""")

spark.sql("SELECT * FROM view_stat_hour").show(24)

spark.sql("SHOW TABLES").show

spark.sql("""
   CREATE OR REPLACE VIEW view_stat_daily AS
      SELECT rentDate as date, 
             rentStatId as statId,
             rentStatName as statName,
             count(*) as count,
             sum(useTime) as timeSum, avg(useTime) as timeAvg,
             sum(useDistance) as distSum, avg(useDistance) as distAvg 
      FROM AiRoBiC
      GROUP BY rentDate, rentStatId, rentStatName
      ORDER BY count DESC
""")

spark.sql("SELECT * FROM view_stat_daily").show()

spark.sql("""
   SELECT CASE WHEN dayofweek(rentDate) = 1 THEN 'weekend'
               WHEN dayofweek(rentDate) = 2 THEN 'working'
               WHEN dayofweek(rentDate) = 3 THEN 'working'
               WHEN dayofweek(rentDate) = 4 THEN 'working'
               WHEN dayofweek(rentDate) = 5 THEN 'working'
               WHEN dayofweek(rentDate) = 6 THEN 'working'
               WHEN dayofweek(rentDate) = 7 THEN 'weekend'
               ELSE 'none'
          END AS dayType,
          rentStatId as statId,
          rentStatName as statName,
          COUNT(*) AS useCount,
          SUM(useTime) AS timeSum, 
          AVG(useTime) AS timeAvg, 
          SUM(useDistance) AS distSum, 
          AVG(useDistance) AS distAvg 
     FROM AiRoBiC
    GROUP BY 1, statId, statName
    ORDER BY useCount DESC
""").show(100)
