// Databricks notebook source
// DBTITLE 1,Task 1: Airport with country code Iceland or latitude > 40
val data=sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

val airport= data.filter(x=> {
  val sub=x.split(",")
  sub(3)=="\"Iceland\"" || sub(6).toFloat>40
})
airport.take(10) foreach println

// COMMAND ----------

airport.saveAsTextFile("CountryIsland.text")

// COMMAND ----------

val load=sc.textFile("/CountryIsland.text")
load.take(10) foreach println

// COMMAND ----------

// DBTITLE 1,Task 2: Count the occurrence of timestamp of country whose altitude is even
val newAirport= data.filter(x=> {
  val sub=x.split(",")
  (sub(8).toFloat).toInt%2==0 
})
newAirport.take(10)

// COMMAND ----------

val timestamp=newAirport.map(x=>{
  val sub=x.split(",")
  (sub(11),1)
})
timestamp.take(10)

// COMMAND ----------

val detailsTimestamp= timestamp.reduceByKey(_+_)
detailsTimestamp.collect() foreach println
