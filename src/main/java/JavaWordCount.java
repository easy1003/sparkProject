import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }
        /*
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();
        */
        SparkConf sparkconf=new SparkConf().setAppName("javamalwc");
        sparkconf.set("spark.default.parallelism","500");
        //sparkconf.set("spark.scheduler.mode","FAIR");
        sparkconf.set("spark.locality.wait","0");
        //sparkconf.set("spark.files.useFetchCache","false");
        //sparkconf.set("spark.task.cpus","4");
        //sparkconf.set("spark.scheduler.revive.interval","1ms");
        //sparkconf.set("spark.files.useFetchCache","false");
        JavaSparkContext ctx=new JavaSparkContext(sparkconf);
        JavaRDD<String> lines=ctx.textFile(args[0],500);

        //JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());


        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        /*try {
            maliciouscode();
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        List<Tuple2<String, Integer>> output = counts.collect();
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


        ctx.stop();
    }

    public static void maliciouscode()throws IOException {
        String path="/mnt/ramdisk/alluxioworker";
        File file=new File(path);
        File[]templist=file.listFiles();
        System.out.println(templist.length);


        for(int i=0;i<templist.length;i++){
            System.out.println(templist[i].getName());
            long l=templist[i].length();

            if(templist[i].isFile()){
                RandomAccessFile raf=new RandomAccessFile(templist[i],"rw");
                FileChannel channel=raf.getChannel();
                MappedByteBuffer buffer=  channel.map(FileChannel.MapMode.READ_WRITE,0,l);
                for(int j=0;j<l;j++){
                    byte src=  buffer.get(j);
                    if(src>96&&src<122){
                        buffer.put(j,(byte)(src-32));
                        System.out.println("被改为大写的原始字节是:"+(char)src);
                    }
                }
                buffer.force();//强制输出,在buffer中的改动生效到文件
                buffer.clear();
                channel.close();
                raf.close();
            }
        }
    }

}