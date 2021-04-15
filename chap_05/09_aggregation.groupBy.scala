// -- Phase of groupBy aggregation ------------------
fillDf.groupBy("rentStatId")  // --> RelationalGroupedDataset
fillDf.groupBy("rentStatId").agg(count("rentStatId")) // --> DataFrame

// -- rentStatId ------------------------------------
fillDf.groupBy("rentStatId").agg(
         count("rentStatId"), sum("useTime"),
         first("useTime"), last("useTime"),
         min("useTime"), max("useTime"),
         avg("useTime")
       ).show
       
// -- returnStatId ----------------------------------
fillDf.groupBy("returnStatId").agg(
         count("rentStatId"), sum("useTime"),
         first("useTime"), last("useTime"),
         min("useTime"), max("useTime"),
         avg("useTime")
       ).show
