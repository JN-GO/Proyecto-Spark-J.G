package proyect.hi

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DF5Optimizado3 {

  def main(args: Array[String]): Unit = {
    // Crea el objeto SparkSession
    val spark = SparkSession.builder
      .master(master = "local[1]")
      .appName("ProyectSpark")
      .getOrCreate()

    // Crea los dataframes con la cantidad de filas especificada
    val df1 = spark.range(1, 4001).toDF("id").withColumn("nombre", lit("a"))
    val df2 = spark.range(1, 1001).toDF("id").withColumn("apellido", lit("A"))
    val df3 = spark.range(1, 11).toDF("id").withColumn("pais", lit("Colombia"))

    // Ajusta el número de particiones según sea necesario
    val numPartitions = 5
    val df1Part = df1.repartition(numPartitions, col("id"))
    val df2Part = df2.repartition(numPartitions, col("id"))
    val df3Part = df3.repartition(numPartitions, col("id"))

    // Realiza las operaciones
    val df4 = df1Part.join(df2Part, Seq("id"), "left")
    val df5 = df3Part.join(df4, Seq("id"))

    // Muestra el resultado
    df5.show()

    // Cierra el SparkSession
    spark.stop()
  }
}
