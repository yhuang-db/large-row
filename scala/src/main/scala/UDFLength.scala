/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object UDFLength {

  def udf_length(text: String): Int = {
    return text.length()
  }

  def main(args: Array[String]): Unit = {
    // dataset
    val dataset = args(0)
    val file = s"/home/huan1531/lr-spark/data/${dataset}.parquet"
    println(s"\n!!!!!!\n${file}\n!!!!!!\n")

    // read parquet file
    val spark = SparkSession.builder.appName("UDF-Length").getOrCreate()
    spark.udf.register("udf_length", udf_length(_: String): Int)
    val df = spark.read.parquet(file)
    df.createOrReplaceTempView("T")
    df.printSchema()
    println(
      s"\n!!!!!!\nNumber of rows: ${df.count()}, Number of columns: ${df.columns.length}\n!!!!!!\n"
    )

    // run sql and write to parquet
    val sql_projection =
      df.columns.map(c => s"udf_length(${c}) as ${c}").mkString(", ")
    val sql = s"SELECT ${sql_projection} FROM T"
    println(s"\n!!!!!!\nSQL: ${sql}\n!!!!!!\n")
    val df_out = spark.sql(sql)
    df_out.write
      .mode("overwrite")
      .parquet(
        "/home/huan1531/lr-spark/data/output/udf_length.parquet"
      )
    df_out.show(1)
    spark.stop()
    println(s"\nudf_length@${dataset}: Success\n")
  }
}
