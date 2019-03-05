import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SendMessage {
    public static void main(String[]args)throws Exception{
        sendMessage(args[0]);

    }
    public static void sendMessage(String sharefile)throws Exception{
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date1=df.format(new Date());
        String messageFileName="msg_"+date1+"send-"+sharefile;
        System.setOut(new PrintStream(new FileOutputStream(messageFileName)));
        System.out.println("begin time: "+date1);
        Long time1=System.currentTimeMillis();
        System.out.println("read start time(timestamp):"+time1);
        String fileready="/sharedata/ready";
        Process process=null;
        long count=0;
        int sign=0;
        while (true){
            if(testTouchExist(fileready)){

                String cmd="/home/master/yyzalluxio2/alluxio/bin/alluxio fs load /sharedata/"+sharefile;
                String cmd2="/home/master/yyzalluxio2/alluxio/bin/alluxio fs free /sharedata/"+sharefile;
                String cmdrmready="/home/master/yyzalluxio2/alluxio/bin/alluxio fs rm /sharedata/ready";
                String cmdcreatedone="/home/master/yyzalluxio2/alluxio/bin/alluxio fs touch /sharedata/done";
                try {
                    System.out.print("file 1Bytes.txt exist!!");
                    System.out.println(" count: "+count+" sign: "+sign);
                    process=Runtime.getRuntime().exec(cmd);
                    try{
                        process.waitFor();
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    if(sign==0){

                        process=Runtime.getRuntime().exec(cmd2);
                        try{
                            process.waitFor();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    process=Runtime.getRuntime().exec(cmdrmready);
                    try{
                        process.waitFor();
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    process=Runtime.getRuntime().exec(cmdcreatedone);
                    try{
                        process.waitFor();
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    System.out.print("   finish a round ");
                    System.out.println("count: "+count+" sign: "+sign+" send: "+sign);
                    sign=(sign+1)%2;
                    count++;
                    Long time2=System.currentTimeMillis();
                    System.out.println("from begin, time(timestamp) diff:"+(time2-time1));
                    long dif = (time2-time1)/1000;
                    double rate = count/((double)dif);
                    System.out.println("receive rate: "+ rate);


                }catch (Exception e){
                    e.printStackTrace();
                }
            }else {
                System.out.println("file 1Bytes.txt does not exist");
            }

        }

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
}
