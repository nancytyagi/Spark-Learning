// Databricks notebook source
// DBTITLE 1,Task 1: All prime numbers 
val ndata= sc.textFile("/FileStore/tables/numberData.csv")

val fdata= ndata.filter(line => !line.startsWith("Number"))
fdata.take(3)

// COMMAND ----------

fdata.collect()

// COMMAND ----------

def isPrime(i:Int):Boolean = 
if(i<=1)
  false
else if(i==2)
  true
else
  !(2 until i).exists(n=> i%n == 0)

// COMMAND ----------

fdata.map(x=>isPrime(x.toInt)).countByValue()

// COMMAND ----------

val primeNumbers=fdata.filter(x=>isPrime(x.toInt))
print("Prime numbers")
primeNumbers.collect() foreach println

// COMMAND ----------

// DBTITLE 1,Task 2: Sum of all the Prime Numbers
val newprime= primeNumbers.map(x=>x.toInt)
val sumPrime= newprime.reduce((x,y)=> x+y)
print("sum of all prime numbers",sumPrime)
