package co.jp.mscg.spark.examples;

import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
}
