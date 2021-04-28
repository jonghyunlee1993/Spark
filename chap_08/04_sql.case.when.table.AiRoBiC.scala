// -- 날짜함수, https://www.obstkel.com/blog/spark-sql-date-functions
spark.sql("""
   SELECT date_format(rentDate, 'EE') as dayName, 
          dayofweek(rentDate) as dayNumb, 
          COUNT(*)
     FROM AiRoBiC
    GROUP BY 1, 2
    ORDER BY 2
""").show         

// -- CASE WHEN: {1,7}은 주말, {2.3.4.5.6}은 주중
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
          COUNT(*) AS useCount,
          SUM(useTime) AS timeSum,      -- 주중과 주말의 이용시간
          AVG(useTime) AS timeAvg,      -- 합계와 평균을 비교해보자
          SUM(useDistance) AS distSum,  -- 나아가 분산과 표준편차 및
          AVG(useDistance) AS distAvg   -- 중간값도 살펴보자 
     FROM AiRoBiC
    GROUP BY 1
""").show 
