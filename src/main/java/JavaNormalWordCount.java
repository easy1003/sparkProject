import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
public class JavaNormalWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date1=df.format(new Date());
        Long time1=System.currentTimeMillis();

        String [] ag = args[0].split("19998/");
        String filename="avaliable/result_"+date1+"-"+ag[ag.length-1]+".txt";

        System.setOut( new PrintStream(new FileOutputStream(filename)));
        System.out.println("read start time:"+date1);
        System.out.println("read start time(timestamp):"+time1);

        /*SparkConf sparkconf=new SparkConf().setAppName("javawc").set("spark.default.parallelism","24");
        sparkconf.set("spark.locality.wait","1");
        JavaSparkContext ctx=new JavaSparkContext(sparkconf);
        JavaRDD<String> lines=ctx.textFile(args[0],24);*/



        String date2=df.format(new Date());
        Long time2=System.currentTimeMillis();
        System.out.println("after textFile and start flatMap time:"+date2);
        System.out.println("after textFile and start flatMap time(timestamp):"+time2);
        System.out.println("after textFile and start flatMap time(timestamp) diff:"+(time2-time1));


        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        String date3=df.format(new Date());
        Long time3=System.currentTimeMillis();
        System.out.println("after flatMap and start mapToPair time:"+date3);
        System.out.println("after flatMap and start mapToPair time(timestamp) :"+time3);
        System.out.println("after flatMap and start mapToPair time(timestamp) diff:"+(time3-time2));



        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        String date4=df.format(new Date());
        Long time4=System.currentTimeMillis();
        System.out.println("after mapToPair and start reduceByKey time:"+date4);
        System.out.println("after mapToPair and start reduceByKey time(timestamp):"+time4);
        System.out.println("after mapToPair and start reduceByKey time(timestamp)diff:"+(time4-time3));


        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        /*try {
            maliciouscode();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        String date5=df.format(new Date());
        Long time5=System.currentTimeMillis();
        System.out.println("after reduceByKey and start collect time:"+date5);
        System.out.println("after reduceByKey and start collect time(timestamp):"+time5);
        System.out.println("after reduceByKey and start collect time(timestamp)diff :"+(time5-time4));


        List<Tuple2<String, Integer>> output = counts.collect();

        String date6=df.format(new Date());
        Long time6=System.currentTimeMillis();
        System.out.println("task end time:"+date6);
        System.out.println("task end time(timestamp):"+time6);
        System.out.println("task end time(timestamp) total diff:"+(time6-time1));

        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        /*
        JavaRDD<String> lines2 = spark.read().textFile(args[1]).javaRDD();
        JavaRDD<String> words2 = lines2.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones2 = words2.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts2 = ones2.reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> output2 = counts2.collect();
        for (Tuple2<?,?> tuple : output2) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        */


        spark.stop();
    }

}