import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Receive {
    public static void main(String[]args)throws Exception{

        receivemessage(args[0]);
    }
    public static void receivemessage(String sharefile)throws Exception{
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date1=df.format(new Date());
        String messageFileName="msg_"+date1+"receive-"+sharefile;
        System.setOut(new PrintStream(new FileOutputStream(messageFileName)));
        System.out.println("begin time: "+date1);
        Long time1=System.currentTimeMillis();
        System.out.println("read start time(timestamp):"+time1);
        System.out.println("Begin Receive...");
        String rex="inMemoryPercentage="+"100";
        String fileready="/sharedata/ready";
        String filedone="/sharedata/done";
        String cmdrmdone="/home/master/yyzalluxio2/alluxio/bin/alluxio fs rm /sharedata/done";
        String cmdcreateready="/home/master/yyzalluxio2/alluxio/bin/alluxio fs touch /sharedata/ready";
        Process process=null;
        long count=0;
        while (true){
            Runtime.getRuntime().exec("/home/master/yyzalluxio2/alluxio/bin/alluxio fs touch /sharedata/ready");
            while(true){
                if(testTouchExist(filedone)){
                    System.out.print("filedone2 exist!!");
                    System.out.println(" count: "+count);
                    try {
                        String cmdtest="/home/master/yyzalluxio2/alluxio/bin/alluxio fs stat /sharedata/"+sharefile;

                        process = Runtime.getRuntime().exec(cmdtest);
                        try{
                            process.waitFor();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
                        String line = "";
                        input.readLine();
                        line = input.readLine();
                        Pattern pattern = Pattern.compile(rex);
                        Matcher m = pattern.matcher(line);
                        if(m.find()){
                            input.close();
                            System.out.print("   count:"+count+"receive: ");
                            System.out.println(1);
                        }else{
                            input.close();
                            System.out.print("   count:"+count+"receive: ");
                            System.out.println(0);
                        }
                        count++;
                        Long time2=System.currentTimeMillis();
                        System.out.println("from begin, time(timestamp) diff:"+(time2-time1));
                        long dif = (time2-time1)/1000;
                        double rate = count/((double)dif);
                        System.out.println("receive rate: "+ rate);
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    process=Runtime.getRuntime().exec(cmdrmdone);
                    try{
                        process.waitFor();
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    process=Runtime.getRuntime().exec(cmdcreateready);
                    try{
                        process.waitFor();
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    System.out.print("   finish a round");
                    System.out.println(" count: "+count);
                }else {
                    System.out.println("done2 doesnot exist");
                }
            }

        }
    }
    public static boolean creatTouch(String filename){
        Process process=null;
        String cmd="/home/master/yyzalluxio2/alluxio/bin/alluxio fs touch "+filename;
        String rex=filename+" has been created";
        Pattern pattern = Pattern.compile(rex);
        try {
            process = Runtime.getRuntime().exec(cmd);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            line = input.readLine();
            Matcher m = pattern.matcher(line);
            if(m.find()){
                input.close();
                return true;
            }else{
                input.close();
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;

    }
    public static boolean testTouchExist(String filename){
        Process process=null;
        String cmd="/home/master/yyzalluxio2/alluxio/bin/alluxio fs du "+filename;
        String rex=filename+" is 0 bytes";
        Pattern pattern = Pattern.compile(rex);
        try {

            process = Runtime.getRuntime().exec(cmd);
            try{
                process.waitFor();
            }catch (Exception e){
                e.printStackTrace();
            }
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            line = input.readLine();
            Matcher m = pattern.matcher(line);
            if(m.find()){
                input.close();
                return true;
            }else{
                input.close();
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;

    }
    public static boolean rmTouchFile(String filename){
        Process process=null;
        String cmd="/home/master/yyzalluxio2/alluxio/bin/alluxio fs rm "+filename;
        String rex=filename+" has been removed";
        Pattern pattern = Pattern.compile(rex);
        try {
            process = Runtime.getRuntime().exec(cmd);
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            line = input.readLine();
            Matcher m = pattern.matcher(line);
            if(m.find()){
                input.close();
                return true;
            }else{
                input.close();
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
