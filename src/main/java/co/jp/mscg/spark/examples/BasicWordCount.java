package co.jp.mscg.spark.examples;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class BasicWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		// SparkContextの作成
		JavaSparkContext sc = new JavaSparkContext("local", "Basic WordCount");

		// $HADOOP_HOMEを環境に合せた値に変更
		JavaRDD<String> texstFile = sc.textFile(System.getenv("HADOOP_HOME") + "\\README.md");
		JavaRDD<String> words = texstFile.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairRDD<String, Integer> wordcounts = ones.reduceByKey((a, b) -> a + b);

		// 処理結果の出力
		wordcounts.saveAsTextFile("data/wordcounts");
	}
}
