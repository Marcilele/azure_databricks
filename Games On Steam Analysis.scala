// Databricks notebook source
// MAGIC %fs ls abfss://datalake@datalakegen2storagelele.dfs.core.windows.net

// COMMAND ----------

// MAGIC %fs ls /mnt/datalake/example/games

// COMMAND ----------

// MAGIC %md
// MAGIC ## Annual Game Release Count
// MAGIC

// COMMAND ----------

val df = spark.read.format("csv")
.option("header", "true")
.option("delimiter", ",")
.load("dbfs:/mnt/datalake/example/games/games.csv")
df.show()

// COMMAND ----------

df.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val annual_release_count = df.filter(col("app_id").isNotNull and (col("app_id") =!= ""))
.groupBy(year(to_date(col("date_release"), "yyyy-MM-dd")).as("year"))
.agg(count("*").alias("count"))
.sort(col("count").desc)

annual_release_count.show()

// COMMAND ----------

annual_release_count.write.format("delta").mode("overwrite").save("/mnt/datalake/delta/games/annual_release_count")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Operating System Breakdown for Game Distribution

// COMMAND ----------

df.printSchema()

// COMMAND ----------

val operating_distribution_count = df.agg(
  sum(when($"win"=== "true", 1).otherwise(0)).alias("win_count"),
  sum(when($"mac" === "true", 1).otherwise(0)).alias("mac_count"),
  sum(when($"linux" === "true", 1).otherwise(0)).alias("linux_count")

)
operating_distribution_count.show()

// COMMAND ----------

operating_distribution_count.write.format("delta").mode("overwrite").save("/mnt/datalake/delta/games/operating_distribution_count")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Correlation Between Game Playtime and User Recommendations

// COMMAND ----------

val df_recommendation = spark.read.format("csv")
.option("header", "true")
.option("delimiter", ",")
.load("dbfs:/mnt/datalake/example/games/recommendations.csv")

// COMMAND ----------

df_recommendation.show()

// COMMAND ----------

val correlation_recommendation_time = df_recommendation.filter(col("is_recommended").isNotNull and (col("is_recommended") =!= ""))
.groupBy("is_recommended")
.agg(round(avg(col("hours")), 2).alias("average_hours_played"))

correlation_recommendation_time.show()

// COMMAND ----------

correlation_recommendation_time.printSchema()

// COMMAND ----------

correlation_recommendation_time.write.format("delta").mode("overwrite").save("/mnt/datalake/delta/games/correlation_recommendation_time")

// COMMAND ----------

// MAGIC %md
// MAGIC ## User Feedback On Steam Games

// COMMAND ----------

df.show()

// COMMAND ----------

val rating_count = df.filter(col("app_id").isNotNull and (col("app_id") =!= ""))
.groupBy("rating")
.count()
.sort(col("count").desc)

// COMMAND ----------

rating_count.show()

// COMMAND ----------

rating_count.write.format("delta").mode("overwrite").save("/mnt/datalake/delta/games/rating_count")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Steam Game Tag Distribution

// COMMAND ----------

val df_game_tag = spark.read.format("json")
.json("dbfs:/mnt/datalake/example/games/games_metadata.json")
df_game_tag.show()

// COMMAND ----------

val explod_df = df_game_tag.select($"app_id", explode($"tags").alias("tag"))
explod_df.show()

// COMMAND ----------

val tag_count =explod_df.groupBy("tag").count().orderBy(desc("count"))
tag_count.show()

// COMMAND ----------

tag_count.write.format("delta").mode("overwrite").save("/mnt/datalake/delta/games/tag_count")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Top 10 Days with the Most Game Reviews on Steam

// COMMAND ----------

df_recommendation.limit(10).show()

// COMMAND ----------

val top_10_day_reviews = df_recommendation.filter(col("app_id").isNotNull and (col("app_id") =!= ""))
.filter(col("date").isNotNull and (col("date") =!= ""))
.select("app_id", "date")
.groupBy("date")
.count()
.sort(desc("count"))
.limit(10)
top_10_day_reviews.show()

// COMMAND ----------

top_10_day_reviews.write.mode("overwrite").format("delta").save("/mnt/datalake/delta/games/top_10_day_reviews")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Insights into User Responses to Steam Recommendation Reviews

// COMMAND ----------

df_recommendation.limit(10).show()

// COMMAND ----------

df_recommendation.printSchema()

// COMMAND ----------

val convert_df = df_recommendation.filter(col("app_id").isNotNull and (col("app_id") =!= ""))
.withColumn("helpful", col("helpful").cast("int"))
.withColumn("funny", col("funny").cast("int"))


// COMMAND ----------

val feedback_df = convert_df.agg(sum("helpful").alias("helpful_count"), sum("funny").alias("funny_count"))

// COMMAND ----------

feedback_df.show()

// COMMAND ----------

feedback_df.write.mode("overwrite").format("delta").save("/mnt/datalake/delta/games/feedback_df")

// COMMAND ----------


