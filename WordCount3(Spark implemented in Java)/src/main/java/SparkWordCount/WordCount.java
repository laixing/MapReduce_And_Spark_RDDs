package SparkWordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;
import java.util.*;
import java.io.Serializable;
import java.util.Iterator;



public class WordCount {
    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);

        JavaRDD<String> input1=input.map(line -> line.replaceAll("[^\\w\\s]|('s|ing|ed) ", " ").toLowerCase());
        JavaRDD<String> words = input1.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaRDD<String> words1 = words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if(s == null || s=="") {
                    return false;
                }
                return true;
            }
        });

        JavaPairRDD<String, Integer> pairs = words1.mapToPair(
                (PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1)
        );
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
                (Function2<Integer, Integer, Integer>) (x, y) -> (x + y)
        );
        JavaPairRDD<Tuple2<Integer, String>, Integer> countInKey = wordCounts.mapToPair(a -> new Tuple2(new Tuple2<Integer, String>(a._2, a._1), null));
        JavaPairRDD<Tuple2<Integer, String>, Integer> wordSortedByCount = countInKey.sortByKey(new TupleComparator(), false);
        wordSortedByCount.saveAsTextFile(outputFile);
    }
}

class TupleComparator implements Comparator<Tuple2<Integer,String>>, Serializable {
    @Override
    public int compare(Tuple2<Integer, String> tuple1, Tuple2<Integer, String> tuple2) {
        return tuple1._1 - tuple2._1;
    }
}
