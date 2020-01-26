import sun.awt.X11.XPoint;
import java.security.SecureRandom;
import java.io.*;
import java.sql.SQLOutput;

public class PointRecGenerator {
    private static SecureRandom random = new SecureRandom();
    public String randXYPoint(){
        int min = 0;
        int max = 40;

        int xValue = min + random.nextInt(max - min);
        int yValue = min + random.nextInt(max - min);
        return  xValue+ ","+ yValue;
    }

    public String randRec(int n){
        int min = 1;
        int max = 10000;
        int hRange = 20;
        int wRange = 5;
        int xValue = min + random.nextInt(max - min);
        int yValue = min + random.nextInt(max - min);
        int height = min + random.nextInt(hRange- min);
        int width = min + random.nextInt(wRange- min);
        return  "r"+n+","+xValue+ ","+ yValue+ ","+height+ ","+width;
    }

    public static void main(String[] args) throws IOException {
        PointRecGenerator p = new PointRecGenerator();

        //write points file
        try{
            String path1 = "./input/datasetP.csv";
            File file1 = new File(path1);
            FileWriter fwPoint = new FileWriter(file1.getAbsoluteFile());
            BufferedWriter bwPoint = new BufferedWriter(fwPoint);
            for (int i = 0; i<= 40; i++){
                bwPoint.write(p.randXYPoint());
                bwPoint.newLine();
            }
            bwPoint.flush();
            fwPoint.close();
            System.out.println("done!");
        }
        catch (Exception e){
            System.out.println(e);
        }

        //write rectangles file
//        try{
//            String path2 = "/Users/Naitan_Chang/Documents/2019 fall/DS503/Project 2/problem2_4/input/datasetR.csv";
//            File file2 = new File(path2);
//            FileWriter fwRec = new FileWriter(file2.getAbsoluteFile());
//            BufferedWriter bwRec = new BufferedWriter(fwRec);
//            for (int i = 0; i<= 4080000; i++){
//                bwRec.write(p.randRec(i));
//                bwRec.newLine();
//            }
//            bwRec.flush();
//            fwRec.close();
//            System.out.println("done!");
//        }
//        catch (Exception e){
//            System.out.println(e);
//        }

    }
}
