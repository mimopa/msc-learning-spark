package co.jp.mscg.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class BasicFilter {

	public static void main(String[] args) {

		// SparkContextの作成
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Basic Filter");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// $HADOOP_HOMEを環境に合せた値に変更
		JavaRDD<String> texstFile = sc.textFile(System.getenv("HADOOP_HOME") + "\\README.md");
		JavaRDD<String> filteredLine = texstFile.filter(
				new Function<String, Boolean>() {
					public Boolean call(String x) { return x.contains("Scala"); }
				}
		);

		// 処理結果の出力
		filteredLine.saveAsTextFile("data/filter");
	}
}
