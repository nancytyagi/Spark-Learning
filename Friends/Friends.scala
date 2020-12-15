// Databricks notebook source
// DBTITLE 1,Task 1: Average friends for each age
val friendrdd= sc.textFile("/FileStore/tables/FriendsData.csv")
friendrdd.take(3)

// COMMAND ----------

val newfriend=friendrdd.filter(x=> ! x.contains("Id"))
newfriend.take(3)

// COMMAND ----------

val splitted=newfriend.map(x=> {
  val sub=x.split(",")
  (sub(2),(1,sub(3).toInt))
})
splitted.take(10)

// COMMAND ----------

val friendcount=splitted.reduceByKey( (x,y) => (x._1+y._1,x._2+y._2))
friendcount.take(3)

// COMMAND ----------

val averagefriendcount=friendcount.mapValues(x=>x._2/x._1)//average number of friends for each age
for ((age,averageFriends) <- averagefriendcount.collect()) println(age + " : " +averageFriends)

// COMMAND ----------

// DBTITLE 1,Task 2: Maximum friends for each age
val split=newfriend.map(x=> {
  val sub=x.split(",")
  (sub(2),(sub(3).toInt))
})
split.take(10)

// COMMAND ----------

split.groupByKey().take(10)

// COMMAND ----------

val initial=0
val seqOp=(x:Int,y:Int)=> if(x>y) x else y
val combOp=(x1:Int,y1:Int)=> if(x1>y1) x1 else y1
split.aggregateByKey(initial)(seqOp,combOp).collect() foreach println
