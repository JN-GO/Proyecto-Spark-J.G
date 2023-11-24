

package proyect.hi

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object ProyectSpark {

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

    // Realiza las operaciones
    val df4 = df1.join(df2, Seq("id"), "left")
    val df5 = df3.join(df4, Seq("id"))

    // Muestra el resultado
    df5.show()

    // Cierra el SparkSession
    spark.stop()
  }
}
