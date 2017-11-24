package co.jp.mscg.spark.examples;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class BasicFlatMap {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Exception("Usage BasicFlatMap sparkMaster inputFile");
		}

		JavaSparkContext sc = new JavaSparkContext(args[0], "basicflatmap", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaRDD<String> rdd = sc.textFile(args[1]);
		JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});
		Map<String, Long> result = words.countByValue();
		for (Entry<String, Long> entry: result.entrySet()) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
	}
}
