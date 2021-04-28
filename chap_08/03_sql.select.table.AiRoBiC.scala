// -- :quit로 세션 종료 및 재시작 후 쿼리 실행
spark.sql("SHOW TABLES").show
spark.sql("DESCRIBE AIRoBiC").show

// -- 테이블 조회 
spark.sql("SELECT * FROM AiRoBiC").show
spark.sql("SELECT rentDate, rentStatId, useTime, useDistance FROM AiRoBiC").show
spark.sql("""
   SELECT * 
     FROM airobic 
    WHERE bicNumber = 'SPB-00265' 
    ORDER BY rentStatId DESC
""").show

spark.sql("""
   SELECT * 
     FROM airobic 
    WHERE bicNumber = 'SPB-00265' 
    ORDER BY rentStatId DESC
""").count

spark.sql("""
   SELECT * 
     FROM airobic 
    WHERE bicNumber = 'SPB-00265' 
    ORDER BY rentStatId DESC
""").show(105)