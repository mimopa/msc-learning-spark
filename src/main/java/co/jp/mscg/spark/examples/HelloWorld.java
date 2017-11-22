package co.jp.mscg.spark.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HelloWorld {
	public static void main(String[] args) {
		try(JavaSparkContext sc = new JavaSparkContext("local", "Hello World")) {
//			JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "World", "!"));
			JavaRDD<String> rdd = sc.textFile("data/data.txt");
			rdd.foreach(val -> System.out.print(val + " "));
		}
	}
}
