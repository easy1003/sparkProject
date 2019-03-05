import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Created by Dong on 2018/1/30.
 */
public class SparkNormalCountAlluxio {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaCount <file>");
            System.exit(1);
        }



        SparkSession spark = SparkSession
                .builder()
                .appName("SparkNormalCountAlluxio")
                .getOrCreate();
/*
        SparkConf sparkconf=new SparkConf().setAppName("javawc").set("spark.default.parallelism","24");
        sparkconf.set("spark.locality.wait","1");
        JavaSparkContext ctx=new JavaSparkContext(sparkconf);
        JavaRDD<String> lines=ctx.textFile(args[0],24);
*/
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date1=df.format(new Date());
        Long time1=System.currentTimeMillis();

        String [] ag = args[0].split("19998/");
        String filename="avaliable/result_"+date1+"-"+ag[ag.length-1]+".txt";

        System.setOut( new PrintStream(new FileOutputStream(filename)));
        System.out.println("read start time:"+date1);
        System.out.println("read start time(timestamp):"+time1);




        String date2=df.format(new Date());
        Long time2=System.currentTimeMillis();
        System.out.println("after textFile and start flatMap time:"+date2);
        System.out.println("after textFile and start flatMap time(timestamp):"+time2);
        System.out.println("after textFile and start flatMap time(timestamp) diff:"+(time2-time1));


        lines.saveAsTextFile(args[1]);

        String date4=df.format(new Date());
        Long time4=System.currentTimeMillis();
        System.out.println("after mapToPair and start reduceByKey time:"+date4);
        System.out.println("after mapToPair and start reduceByKey time(timestamp):"+time4);
        System.out.println("after mapToPair and start reduceByKey time(timestamp)diff:"+(time4-time2));


        long result=lines.count();

        String date6=df.format(new Date());
        Long time6=System.currentTimeMillis();
        System.out.println("task end time:"+date6);
        System.out.println("task end time(timestamp):"+time6);
        System.out.println("task end time(timestamp) total diff:"+(time6-time1));

        System.out.println(result);

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