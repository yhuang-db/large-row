/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object UDFUpper {

  def udf_upper(text: String): String = {
    return text.toUpperCase()
  }

  def main(args: Array[String]): Unit = {
    // dataset
    val dataset = args(0)
    val file = s"/home/huan1531/lr-spark/data/${dataset}.parquet"
    println(s"\n!!!!!!\n${file}\n!!!!!!\n")

    // read parquet file
    val spark = SparkSession.builder.appName("UDF-Upper").getOrCreate()
    spark.udf.register("udf_upper", udf_upper(_: String): String)
    val df = spark.read.parquet(file)
    df.createOrReplaceTempView("T")
    df.printSchema()
    println(
      s"\n!!!!!!\nNumber of rows: ${df.count()}, Number of columns: ${df.columns.length}\n!!!!!!\n"
    )

    // run sql and write to parquet
    val sql_projection =
      df.columns.map(c => s"udf_upper(${c}) as ${c}").mkString(", ")
    val sql = s"SELECT ${sql_projection} FROM T"
    println(s"\n!!!!!!\nSQL: ${sql}\n!!!!!!\n")
    val df_out = spark.sql(sql)
    df_out.write
      .mode("overwrite")
      .parquet(
        "/home/huan1531/lr-spark/data/output/udf_upper.parquet"
      )
    df_out.show(1)
    spark.stop()
    println(s"\nudf_upper@${dataset}: Success\n")
  }
}
