import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * <pre>
 * bin/run-example JavaPageRank data/mllib/pagerank_data.txt 10
 * </pre>
 */
public final class JavaPageRank {
    private static final Pattern SPACES = Pattern.compile("\\s+");

    static void showWarning() {
        String warning = "WARN: This is a naive implementation of PageRank " +
                "and is given as an example! \n" +
                "Please use the PageRank implementation found in " +
                "org.apache.spark.graphx.lib.PageRank for more conventional use.";
        System.err.println(warning);
    }

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
            System.exit(1);
        }

        showWarning();
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date1=df.format(new Date());
        Long time1=System.currentTimeMillis();
        String filename="result_"+time1+".txt";

        System.setOut( new PrintStream(new FileOutputStream(filename)));
        System.out.println("start time:"+date1);
        System.out.println(time1);

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaPageRank")
                .getOrCreate();

        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        // Loads all URLs from input file and initialize their neighbors.
        /*JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] parts = SPACES.split(s);
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey().cache();
        */
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] parts = SPACES.split(s);
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey();

        if(args.length==3){
            System.out.println("alluxio cache");
            links.saveAsTextFile(args[2]);
        }else if(args.length==2){
            System.out.println("spark cache");
            links.cache();
        }

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < Integer.parseInt(args[1]); current++) {
            // Calculates URL contributions to the rank of other URLs.
            System.out.println("current:"+current);
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1()) {
                            results.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    });

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
        }

        // Collects all URL ranks and dump them to console.
        List<Tuple2<String, Double>> output = ranks.collect();
        String date2=df.format(new Date());
        Long time2=System.currentTimeMillis();
        System.out.println("end time:"+date2);
        System.out.println(time2);

        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
        }

        spark.stop();
    }
}