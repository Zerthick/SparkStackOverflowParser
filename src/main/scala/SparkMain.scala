import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkMain {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length != 3) {

      println("Correct usage: scala SparkMain <master> <input.xml> <output.xml>")

      return
    }

    val spark = SparkSession
      .builder()
      .appName("Word Count XML")
      .master(args(0))
      .getOrCreate()

    import spark.implicits._

    val startTime = System.currentTimeMillis()

    val inputFile = spark.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "posts")
      .option("rowTag", "row")
      .load(args(1))

    val cleanedPosts = inputFile.select("_ID", "_Body")
      .map(row => (row.getLong(0) , row.getString(1).toLowerCase.replaceAll("\\s+", " ").replaceAll("(<.*?>|['\"])", "")))
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "body").cache()

    val ds = cleanedPosts.as[(Long, String)]

    val splitWords = ds.flatMap {
      case (x1, x2) => x2.split("\\b").map((x1, _))
    }.toDF("id", "word")

    val filteredWords = splitWords.map(row => (row.getLong(0), row.getString(1).trim)).filter(row => row._2.length > 2 && row._2.matches("[a-z]+.*?"))
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "word").cache()

    val tf = filteredWords.select("word").map(row => (row.getString(0), 1)).groupBy("_1").sum("_2")
      .withColumnRenamed("_1", "wordtf")
      .withColumnRenamed("sum(_2)", "ctf").as("tf").cache()

    val df = filteredWords.distinct().select("word").map(row => (row.getString(0), 1)).groupBy("_1").sum("_2")
      .withColumnRenamed("_1", "worddf")
      .withColumnRenamed("sum(_2)", "df").as("df")

    val termCount = tf.count()
    val docCount = cleanedPosts.count()

    val result = tf.join(df, $"tf.wordtf" === $"df.worddf").select("wordtf", "ctf", "df")
      .map(row => (row.getString(0), row.getLong(1), row.getLong(2), Math.log10(docCount / row.getLong(2)), Math.log10(termCount / row.getLong(1))))
      .withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "ctf")
      .withColumnRenamed("_3", "df")
      .withColumnRenamed("_4", "idf")
      .withColumnRenamed("_5", "ictf")

    result.write
      .format("com.databricks.spark.xml")
      .option("rootTag", "posts")
      .option("rowTag", "row")
      .save(args(2))

    val endTime = System.currentTimeMillis()

    println("Total Runtime: " + (endTime - startTime))
  }
}
