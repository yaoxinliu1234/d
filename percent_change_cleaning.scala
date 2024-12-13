
val filePath = "gs://nyu-dataproc-hdfs-ingest/Percent_Change_in_Consumer_Spending copy.csv"
val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePath)
df.printSchema()
df.show(5)
import org.apache.spark.sql.functions._
val nullCounts = df.select(df.columns.map(c => count(when(col(c).isNull || col(c) === "", c)).alias(c)): _*)
nullCounts.show()
df.describe().show()
val duplicateCount = df.except(df.distinct()).count()
df.printSchema()
df.count()
df.columns.length
df.describe().show()
df.select("State FIPS code", "All merchant category codes spending", "Date").describe().show()
df.select( "All merchant category codes spending", "Date").describe().show()
df.select("Grocery and food store (GRF)  spending").describe().show()
df.select("Grocery and food store (GRF)  spending","All merchant category codes spending").describe().show()
//change data type for date 

val dfWithDate = df.withColumn("Date", to_date(col("Date"), "MM/dd/yyyy"))

dfWithDate.printSchema()

//rename the varibles
 val columnMapping = Map(
     |   "State FIPS code" -> "StateCode",
     |   "All merchant category codes spending" -> "AllSpending",
     |   "Accommodation and food service (ACF) spending" -> "FoodServiceSpending",
     |   "Arts, entertainment, and recreation (AER)  spending" -> "EntertainmentSpending",
     |   "General merchandise stores (GEN) and apparel and accessories (AAP) spending" -> "MerchandiseSpending",
     |   "Grocery and food store (GRF)  spending" -> "GrocerySpending",
     |   "Health care and social assistance (HCS) spending " -> "HealthSpending",
     |   "Transportation and warehousing (TWS)  spending" -> "TransportSpending",
     |   "Retail spending, including grocery  (AAP, CEC, GEN, GRF, HIC, ETC, SGH) " -> "RetailIncGrocery",
     |   "Retail spending, excluding grocery ((AAP, CEC, GEN, HIC, ETC, SGH)" -> "RetailExGrocery"
     | )

     val renamedDFWithDate = columnMapping.foldLeft(dfWithDate) { case (tempDF, (oldName, newName)) =>
     |   tempDF.withColumnRenamed(oldName, newName)
     | }


renamedDFWithDate.printSchema()
val finalRenamedDF = renamedDFWithDate
finalRenamedDF.printSchema()
finalRenamedDF.columns.foreach(println)
val finalRenamedDFUpdated = finalRenamedDF.withColumnRenamed("Retail spending, excluding grocery ((AAP, CEC, GEN, HIC, ETC, SGH)","RetailExGrocery")
finalRenamedDFUpdated.printSchema()
val updatedColumns = finalRenamedDF.columns.map {
     |   case col if col.contains("Retail spending, excluding grocery") => "RetailExGrocery"
     |   case other => other
     | }

     val finalRenamedDFUpdated = finalRenamedDF.toDF(updatedColumns: _*)
     finalRenamedDFUpdated.printSchema()
     //save the data to hdfs
     finalRenamedDFUpdated.write.format("csv").option("header","true").mode("overwrite").save("/user/yl5739_nyu_edu/percent")

