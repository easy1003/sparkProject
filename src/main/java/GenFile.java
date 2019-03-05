import java.io.FileWriter;

public class GenFile {
    public static void main(String[]args)throws Exception{
        System.out.print("test");
        gensmallfile(512);
    }
    static void genfile(int size) throws Exception{
        String filename;
        if (size>=1024&&size<1024*1024){
            filename = (size/1024)+"MB.txt";
        } else if (size>=1024*1024){
            filename = (size/(1024*1024))+"GB.txt";
        } else {
            filename = size+"KB.txt";
        }
        FileWriter fileWriter = new FileWriter("/home/master/extend/genfile/"+filename);
        String content = "1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"1111122222333334444455555666667777788888999990000\n";
        content=content+"11111222223333344444555\n";
        for(int i=0;i<size;i++){
            fileWriter.write(content);
            fileWriter.flush();
        }
        fileWriter.close();
    }

    static void gensmallfile(int size) throws Exception{
        String filename;
        filename = size+"Bytes.txt";

        FileWriter fileWriter = new FileWriter("/home/master/extend/genfile/"+filename);
        for(int i=0;i<size;i++){
            fileWriter.write("1");
            fileWriter.flush();
        }
        fileWriter.close();
    }
}
