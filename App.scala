package org.example
import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark
import org.apache.spark.sql.functions.{col, count, countDistinct, desc, hour, regexp_extract}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object App {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("NASA Weblogs.com")
      .getOrCreate()

    val frame1 = spark.read.text("C:\\Users\\aitor.malo\\Desktop\\NASA-Weblogs\\datasets\\access_log_Aug95.txt")
    val frame2 = spark.read.text("C:\\Users\\aitor.malo\\Desktop\\NASA-Weblogs\\datasets\\access_log_Jul95.txt")
    val union_df = frame1.union(frame2).toDF()

    val simpleSchema = StructType(Array(
      StructField("remotehost ", StringType, true),
      StructField("rfc931 ", StringType, true),
      StructField("authuser ", StringType, true),
      StructField("date", StringType, true),
      StructField("request", StringType, true),
      StructField("status", StringType, true),
      StructField("bytes", StringType, true)
    ))

    import spark.implicits._
    val parsed_df = union_df.select(regexp_extract($"value", """^([^(\s|\ -)]+)""", 1).alias("remotehost"),
      regexp_extract($"value", """^.*\[(\d\d\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2})""", 1).alias("date"),
      regexp_extract($"value", """^.*\"(\S+)\s(\S+)\s*(\S*)\"""", 1).alias("method"),
      regexp_extract($"value", """^.*\w+\s+([^\s]+)\s+HTTP.*"""", 1).alias("path"),
      regexp_extract($"value", """(\d{3}\s*)+""", 1).alias("status"),
      regexp_extract($"value", """([0-9]+)$""", 1).alias("bytes"),
    );

    parsed_df.printSchema()

    parsed_df.select(countDistinct("method")).show()

    parsed_df.groupBy("status").count().orderBy(desc("count")).show()

    parsed_df.groupBy("path").count().orderBy(desc("count")).show()

    parsed_df.groupBy("bytes").count().orderBy(desc("count")).show(1)

    parsed_df.groupBy("remotehost").count().orderBy(desc("count")).show(15)

    parsed_df.groupBy("date").count().orderBy(desc("count")).show()

    parsed_df.where(parsed_df("status")==="404").agg(count("status").as("Número de veces")).show()

    parsed_df.groupBy(hour(col("date").cast(DateType))).count().orderBy(desc("count")).show(15)

  }
}
