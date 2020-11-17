// Databricks notebook source
val nums = List.range(1,46)

// COMMAND ----------

val div4int = nums.filter((x: Int) => x % 4 == 0)
println("Sum of the numbers divisible by 4 is "+div4int.sum)

// COMMAND ----------

val div3intless20 = nums.filter((x: Int) => x % 3 == 0).filter((x:Int) => x < 20)

def squares(x: Int) : Int = {x * x}
def applyFunc(x: Int, f: Int => Int): Int = {
  f(x)
}

println("Sum of the squares of the numbers divisible by 3 and less than 20 is "+
  (div3intless20.map(squares)).sum)

// COMMAND ----------

//Write a max function that picks the max of two numbers and another get_max
//function to call the first one with inputs.


