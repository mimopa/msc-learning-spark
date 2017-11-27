package co.jp.mscg.spark.examples;

import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaSparkSQLExample {

	public static class Person implements Serializable {
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}
	}

	// メイン処理
	public static void main(String[] args) throws AnalysisException {
		// SparkSQLを使うときのお約束！
		SparkSession spark = SparkSession.builder().master("local").appName("Java Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate();

		runBasicDataFrameExample(spark);
		runDatasetCreationExample(spark);
		runInferSchemaExample(spark);
		runProgrammaticSchemaExample(spark);
		// 最後はセッションを破棄しておく
		spark.stop();

	}

	// jsopnからDataFrameを作成して操作するサンプルメソッド
	private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
		Dataset<Row> df = spark.read().json("./data/people.json");

		// 標準出力に読み込んだコンテンツを表示
		df.show();

		// Print the schema in a tree format
		df.printSchema();

		// Select only the "name" column
		df.select("name").show();

		// Select everybody, but increment the age by 1
		df.select(col("name"), col("age").plus(1)).show();

		// Select people older than 21
		df.filter(col("age").gt(21)).show();

		// Count people byu age
		// Countすると、シャッフルされるようだ
		df.groupBy("age").count().show();

		// Register the DataFrame as a SQL temporary view
		// 一時的にpeopleテーブルを作成する
		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();

		// Register the DataFrame as a blobal tempoorary view
		// グローバルテンポラリーとしてテーブルを作成することも可能
		df.createGlobalTempView("people");

		// Global temporary view is tied to a system preserved database `global_temp`
		spark.sql("SELECT * FROM global_temp.people").show();

		// Global temprary view is cross-session
		// 新しくセッションを開始しても同じテーブルを参照できる
		spark.newSession().sql("SELECT * FROM global_temp.people").show();

	}

	private static void runDatasetCreationExample(SparkSession spark) {

		// Create an instance opf a Bean class
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);

		// Encoders are created for Java beans
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		javaBeanDS.show();

		// Encoders for most common types are provided in class Encoders
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1, integerEncoder);
		transformedDS.collect();

		// DataFrames can be converted to a Dataset by providing a class. Mapping based on name
		String path = "./data/people.json";
		Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
		peopleDS.show();


	}

	private static void runInferSchemaExample(SparkSession spark) {
		// Create an RDD of Person objects from a text file
		JavaRDD<Person> peopleRDD = spark.read()
				.textFile("./data/people.txt")
				.javaRDD()
				.map(line -> {
					String[] parts = line.split(",");
					Person person = new Person();
					person.setName(parts[0]);
					person.setAge(Integer.parseInt(parts[1].trim()));
					return person;
				});

		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
		// Register the DataFrame as a temporary view
		peopleDF.createOrReplaceTempView("people");

		// SQL statements can be run by using the sql methods provided by spark
		Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

		// The columns of a row in the result can be accessed by field index
		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> teenagerNamesByIndexDF = teenagersDF.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), stringEncoder);
		teenagerNamesByIndexDF.show();

		// or by field name
		Dataset<String> teenagerNamesByFieldDF = teenagersDF.map((MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"), stringEncoder);
		teenagerNamesByFieldDF.show();


	}

	private static void runProgrammaticSchemaExample(SparkSession spark) {
		// Create an RDD
		JavaRDD<String> peopleRDD = spark.sparkContext()
				.textFile("./data/people.txt", 1)
				.toJavaRDD();

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows
		JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
			String[] attributes = record.split(" ");
			return RowFactory.create(attributes[0], attributes[1].trim());
		});

		// Apply the schema to the RDD
		Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

		// Creates a temporary view using the DataFrame
		peopleDataFrame.createOrReplaceTempView("people");

		// SQL can be run over a temporary view created using DataFrame
		Dataset<Row> results = spark.sql("SELECT name FROM people");

		// The results of SQL queries are DataFrames and support all the normal RDD operations
		// The columns of a row in the result can be accessed by field index or by field name
		Dataset<String> namesDS = results.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), Encoders.STRING());
		namesDS.show();
	}
}
