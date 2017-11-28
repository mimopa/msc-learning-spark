package co.jp.mscg.spark.examples;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSQLDataSourceExample {
	// データソース
	// Spark SQLはDataFrameインタフェースを使って様々なデータソース上での操作をサポートします。データフレームは
	// relational transformationを使って操作することができ、一時viewを生成するために使うことができます。
	// データフレームを一時的なビューとして登録することによりデータにSQLクエリを実行することができます。
	// このセクションではSparkデータソースを使ってデータをロードおよび保存する一時的なメソッドを説明し、
	// その後ビルトインのデータソースのために利用可能な特定のオプションについて詳しく調べます。

	public static class Square implements Serializable {
		private int value;
		private int square;

		// Getters and setters...
		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getSquare() {
			return square;
		}

		public void setSquare(int square) {
			this.square = square;
		}
	}

	public static class Cube implements Serializable {
		private int value;
		private int cube;

		// Getters and setters...
		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getCube() {
			return cube;
		}

		public void setCube(int cube) {
			this.cube = cube;
		}
	}

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.master("local")
				.appName("Java Spark SQL data source example")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		spark.stop();
	}

	private static void runBasicDataSourceExample(SparkSession spark) {
		Dataset<Row> usersDF = spark.read().load("./data/users.parquet");
		usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");

		Dataset<Row> peopleDF = spark.read().format("json").load("./data/people.json");
		peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquet.`./data/users.parquet`");

		peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");

		usersDF
			.write()
			.partitionBy("favorite_color")
			.format("parquet")
			.save("namesPartByColor.parquet");

		peopleDF
			.write()
			.partitionBy("favorite_color")
			.bucketBy(42, "name")
			.saveAsTable("people_partitioned_backeted");

		spark.sql("DROP TABLE IF EXISTS people_backeted");
		spark.sql("DROP TABLE IF EXISTS people_partitioned_backeted");
	}

	private static void runBasicParquetExample(SparkSession spark) {

		Dataset<Row> peopleDF = spark.read().json("./data/people.json");

		peopleDF.write().parquet("people.parquet");

		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");

		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHER age BETWEEN 13 AND 19");
		Dataset<String> namesDS = namesDF.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), Encoders.STRING());
		namesDS.show();
	}

	private static void runParquetSchemaMergingExample(SparkSession spark) {

		List<Square> squares = new ArrayList<>();
		for (int value = 1; value <=5; value++) {
			Square square = new Square();
			square.setValue(value);
			square.setSquare(value * value);
			squares.add(square);
		}

		// Create a simple DataFrame, store into a partition directory
		Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
		squaresDF.write().parquet("./data/test_tabel/key=1");

		List<Cube> cubes = new ArrayList<>();
		for (int value = 6; value <= 10; value++) {
			Cube cube = new Cube();
			cube.setValue(value);
			cube.setCube(value * value * value);
			cubes.add(cube);
		}

		// Create another DataFrame in a new partition directory,
		// adding a new column and dropping an existing column
		Dataset<Row> cubesDF = spark.createDataFrame(cubes,  Cube.class);
		cubesDF.write().parquet("./data/test_table/key=2");

		// Read the partitioned table
		Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("./data/test_table");
		mergedDF.printSchema();
	}

	private static void runJsonDatasetExample(SparkSession spark) {

		// A JSON dataset is pointed to by path.
		// The path can be either a single text file or a directory storing text files
		Dataset<Row> people = spark.read().json("./data/people.json");

		// The inferred schema can be visualized using the printSchema() method
		people.printSchema();

		// Creates a temporary view using the DataFrame
		people.createOrReplaceTempView("people");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
		namesDF.show();

		// Alternatively, a DataFrame can be created for a JSON dataset represented by
		// a Dataset<String> storing one JSON object per string.
		List<String> jsonData = Arrays.asList("{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
		anotherPeople.show();
	}

	private static void runJdbcDatasetExample(SparkSession spark) {
		// Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
		// Loading data from a JDBC source
		Dataset<Row> jdbcDF = spark.read()
				.format("jdbc")
				.option("url", "jdbc:postgresql:driver")
				.option("dbtable", "schema.tablename")
				.option("user", "username")
				.option("password", "password")
				.load();

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "username");
		connectionProperties.put("password", "password");
		Dataset<Row> jdbcDF2 = spark.read()
				.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

		// Saving data to a JDBC source
		jdbcDF.write()
			.format("jdbc")
			.option("url", "jdbc:postgresql:dbserver")
			.option("dbtable", "schema.tblename")
			.option("user", "username")
			.option("password", "password")
			.save();

		jdbcDF2.write()
			.option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
			.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
	}

}
