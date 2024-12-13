val dataWithYearMonth = data
  .withColumn("YearMonth", concat_ws("-", year(to_date(col("Date"), "yyyy-MM-dd")), month(to_date(col("Date"), "yyyy-MM-dd"))))

val monthlyAggregatedData = dataWithYearMonth
  .groupBy("YearMonth")
  .agg(
    avg("AllSpending").alias("AvgPctChangeAllSpending"),
    avg("FoodServiceSpending").alias("AvgPctChangeFoodService"),
    avg("EntertainmentSpending").alias("AvgPctChangeEntertainment"),
    avg("MerchandiseSpending").alias("AvgPctChangeMerchandise"),
    avg("GrocerySpending").alias("AvgPctChangeGrocery"),
    avg("HealthSpending").alias("AvgPctChangeHealth"),
    avg("TransportSpending").alias("AvgPctChangeTransport"),
    avg("RetailIncGrocery").alias("AvgPctChangeRetailIncGrocery"),
    avg("RetailExGrocery").alias("AvgPctChangeRetailExGrocery")
  )
val orderedMonthlyAggregatedData = monthlyAggregatedData.orderBy("YearMonth")
orderedMonthlyAggregatedData.show(false)




val dataWithYearMonthSplit = orderedMonthlyAggregatedData
  .withColumn("Year", split(col("YearMonth"), "-").getItem(0).cast("int"))
  .withColumn("Month", split(col("YearMonth"), "-").getItem(1).cast("int"))

val dataWithSeasons = dataWithYearMonthSplit
  .withColumn(
    "Season",
    when(col("Month").isin(12, 1, 2), "Winter")
      .when(col("Month").isin(3, 4, 5), "Spring")
      .when(col("Month").isin(6, 7, 8), "Summer")
      .otherwise("Fall")
  )

val seasonalAggregatedData = dataWithSeasons
  .groupBy("Year", "Season")
  .agg(
    avg("AvgPctChangeAllSpending").alias("AvgPctChangeAllSpending"),
    avg("AvgPctChangeFoodService").alias("AvgPctChangeFoodService"),
    avg("AvgPctChangeEntertainment").alias("AvgPctChangeEntertainment"),
    avg("AvgPctChangeMerchandise").alias("AvgPctChangeMerchandise"),
    avg("AvgPctChangeGrocery").alias("AvgPctChangeGrocery"),
    avg("AvgPctChangeHealth").alias("AvgPctChangeHealth"),
    avg("AvgPctChangeTransport").alias("AvgPctChangeTransport"),
    avg("AvgPctChangeRetailIncGrocery").alias("AvgPctChangeRetailIncGrocery"),
    avg("AvgPctChangeRetailExGrocery").alias("AvgPctChangeRetailExGrocery")
  )

val orderedSeasonalData = seasonalAggregatedData
  .withColumn("SeasonOrder", when(col("Season") === "Winter", 1)
    .when(col("Season") === "Spring", 2)
    .when(col("Season") === "Summer", 3)
    .otherwise(4))
  .orderBy("Year", "SeasonOrder")
  .drop("SeasonOrder")



orderedSeasonalData.show(false)



//highest 

val dataWithMaxContribution = orderedSeasonalData
  .withColumn(
    "MaxContribution",
    greatest(
      col("AvgPctChangeAllSpending"),
      col("AvgPctChangeFoodService"),
      col("AvgPctChangeEntertainment"),
      col("AvgPctChangeMerchandise"),
      col("AvgPctChangeGrocery"),
      col("AvgPctChangeHealth"),
      col("AvgPctChangeTransport"),
      col("AvgPctChangeRetailIncGrocery"),
      col("AvgPctChangeRetailExGrocery")
    )
  )

val dataWithMaxCategory = dataWithMaxContribution
  .withColumn(
    "TopCategory",
    when(col("MaxContribution") === col("AvgPctChangeAllSpending"), "AllSpending")
      .when(col("MaxContribution") === col("AvgPctChangeFoodService"), "FoodService")
      .when(col("MaxContribution") === col("AvgPctChangeEntertainment"), "Entertainment")
      .when(col("MaxContribution") === col("AvgPctChangeMerchandise"), "Merchandise")
      .when(col("MaxContribution") === col("AvgPctChangeGrocery"), "Grocery")
      .when(col("MaxContribution") === col("AvgPctChangeHealth"), "Health")
      .when(col("MaxContribution") === col("AvgPctChangeTransport"), "Transport")
      .when(col("MaxContribution") === col("AvgPctChangeRetailIncGrocery"), "RetailIncGrocery")
      .when(col("MaxContribution") === col("AvgPctChangeRetailExGrocery"), "RetailExGrocery")
      .otherwise("Unknown")
  )


val topCategoryPerSeason = dataWithMaxCategory
  .select("Year", "Season", "TopCategory", "MaxContribution")

topCategoryPerSeason.orderBy("Year", "Season").show(false)



//wiiner of the year 

val yearlyAverages = orderedSeasonalData
  .groupBy("Year")
  .agg(
    avg("AvgPctChangeAllSpending").alias("YearAvgAllSpending"),
    avg("AvgPctChangeFoodService").alias("YearAvgFoodService"),
    avg("AvgPctChangeEntertainment").alias("YearAvgEntertainment"),
    avg("AvgPctChangeMerchandise").alias("YearAvgMerchandise"),
    avg("AvgPctChangeGrocery").alias("YearAvgGrocery"),
    avg("AvgPctChangeHealth").alias("YearAvgHealth"),
    avg("AvgPctChangeTransport").alias("YearAvgTransport"),
    avg("AvgPctChangeRetailIncGrocery").alias("YearAvgRetailIncGrocery"),
    avg("AvgPctChangeRetailExGrocery").alias("YearAvgRetailExGrocery")
  )




val yearlyMaxAvg = yearlyAverages
  .withColumn(
    "MaxAvgPercent",
    greatest(
      col("YearAvgAllSpending"),
      col("YearAvgFoodService"),
      col("YearAvgEntertainment"),
      col("YearAvgMerchandise"),
      col("YearAvgGrocery"),
      col("YearAvgHealth"),
      col("YearAvgTransport"),
      col("YearAvgRetailIncGrocery"),
      col("YearAvgRetailExGrocery")
    )
  )


val yearlyWinnerByAvg = yearlyMaxAvg
  .withColumn(
    "WinnerCategory",
    when(col("MaxAvgPercent") === col("YearAvgAllSpending"), "AllSpending")
      .when(col("MaxAvgPercent") === col("YearAvgFoodService"), "FoodService")
      .when(col("MaxAvgPercent") === col("YearAvgEntertainment"), "Entertainment")
      .when(col("MaxAvgPercent") === col("YearAvgMerchandise"), "Merchandise")
      .when(col("MaxAvgPercent") === col("YearAvgGrocery"), "Grocery")
      .when(col("MaxAvgPercent") === col("YearAvgHealth"), "Health")
      .when(col("MaxAvgPercent") === col("YearAvgTransport"), "Transport")
      .when(col("MaxAvgPercent") === col("YearAvgRetailIncGrocery"), "RetailIncGrocery")
      .when(col("MaxAvgPercent") === col("YearAvgRetailExGrocery"), "RetailExGrocery")
      .otherwise("Unknown")
  )

val yearlyWinnerResults = yearlyWinnerByAvg
  .select("Year", "WinnerCategory", "MaxAvgPercent")

yearlyWinnerResults.orderBy("Year").show(false)


yearlyWinnerResults.write.format("csv").option("header", "true").mode("overwrite").save("/user/yl5739_nyu_edu/yearly_winner_by_avg/")
                                                                                
 topCategoryPerSeason.write.format("csv").option("header", "true").mode("overwrite").save("/user/yl5739_nyu_edu/seasonal_winner_by_contribution/")