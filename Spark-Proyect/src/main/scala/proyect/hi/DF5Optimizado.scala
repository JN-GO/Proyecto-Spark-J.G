package proyect.hi

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DF5Optimizado {

  def main(args: Array[String]): Unit = {
    // Crea el objeto SparkSession
    val spark = SparkSession.builder
      .master(master = "local[1]")
      .appName("ProyectSpark")
      .getOrCreate()

    // Configura el número de particiones
    spark.conf.set("spark.sql.shuffle.partitions", "10")

    // Crea los dataframes con la cantidad de filas especificada
    val df1 = spark.range(1, 4001).toDF("id").withColumn("nombre", lit("a"))
    val df2 = spark.range(1, 1001).toDF("id").withColumn("apellido", lit("A"))
    val df3 = spark.range(1, 11).toDF("id").withColumn("pais", lit("Colombia"))

    // Realiza las operaciones
    // Broadcast join para df3
    val df4 = df1.join(broadcast(df2), Seq("id"), "left")
    val df5 = df3.join(broadcast(df4), Seq("id"))

    // Muestra el resultado
    df5.show()

    // Cierra el SparkSession
    spark.stop()
  }
}
