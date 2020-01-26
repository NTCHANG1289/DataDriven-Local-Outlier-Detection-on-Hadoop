import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;
import org.eclipse.jdt.internal.compiler.ast.DoubleLiteral;
import sun.awt.X11.XPoint;

import java.io.*;
import java.security.Key;
import java.util.*;
import java.util.Comparator;

//
public class LRD {

    public static class OutlierMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] data= value.toString().split("__");

            String point = data[0];
            String[] neighbors = data[1].split("--");
            String Nreach = data[2];

            double p_x = Double.parseDouble(point.split(",")[0]);
            double p_y = Double.parseDouble(point.split(",")[1]);
            double dist = 0;
            for (String neighbor: neighbors){
                double n_x = Double.parseDouble(neighbor.split(",")[0]);
                double n_y = Double.parseDouble(neighbor.split(",")[1]);
                dist = Math.sqrt(Math.pow((p_x-n_x),2) + Math.pow((p_y-n_y),2));
                context.write(new Text(point), new Text(neighbor+"__"+dist+"__"+Nreach));
            }
        }
    }

    public static class OutlierReducer extends Reducer<Text,Text,Text,Text> {


        public double[] StringsToDoubles(String[] strs){
            double[] res = new double[strs.length];
            for (int i=0; i<strs.length; i++){
                res[i] = Double.parseDouble(strs[i]);
            }
            return res;
        }

        public int[] sortindex(double[] dist){
            int[] res = new int[dist.length];
            Double[][] tmp = new Double[dist.length][2];
            for (int i = 0; i<dist.length; i++){
                tmp[i][0] = dist[i];
                tmp[i][1] = (double)i;
            }
            Comparator<Double[]> arrayComparator = new Comparator<Double[]>() {
                @Override
                public int compare(Double[] o1, Double[] o2) {
                    return o1[0].compareTo(o2[0]);
                }
            };
            Arrays.sort(tmp, arrayComparator);

            for (int i=0; i<dist.length; i++){
                double r = tmp[i][1];
                res[i] = (int)r;
            }
            return res;
        }
        public double distab(double x1, double y1, double x2, double y2){
            return Math.sqrt(Math.pow((x1-x2),2) + Math.pow((y1-y2),2));
        }

        private List<String> inform_file = new ArrayList<String>();
        private HashMap<String,String> Kdis_profile = new HashMap<>();
        public void setup(Context context) {
            try{
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    inform_file.clear();
                    try {
                        while ((line = cacheReader.readLine()) != null) {
                            inform_file.add(line);
                        }
                    } finally {
                        cacheReader.close();
                    }
                }
            } catch (IOException e) {
                System.out.println(e);
            }
            for (String line: inform_file){
                String[] dat = line.split("__");
                Kdis_profile.put(dat[0], dat[1]);
            }

        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double reach = 0;
            double reach_sum = 0;
            int Nreach = 0;
            for (Text value: values) {
                String[] data = value.toString().split("__");
                String neighbor = data[0];
                String dist = data[1];
                Nreach = Integer.parseInt(data[2]);
                String nei_Kdis = Kdis_profile.get(neighbor+",");
                reach = Math.max(Double.parseDouble(dist), Double.parseDouble(nei_Kdis));
                reach_sum += reach;
                //context.write(new Text(key), new Text(neighbor+"__"+nei_Kdis+"__"+dist+"__"+Nreach));
            }
            double LRD = Nreach/reach_sum;
            String kk = key.toString().split(",")[0] + "," +key.toString().split(",")[1];
            context.write(new Text(kk), new Text("__"+String.valueOf(LRD)));
        }
    }


    public static void main(String[] args) throws Exception{
        String inputPath = "./out_2nd_neighbor/";
        String outputPath = "./out_LRD/";
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = new Job(conf, "LRD");


        String path1 = "./out_2nd_K_DIS/part-r-00000";
        DistributedCache.addCacheFile(new Path(path1).toUri(), job.getConfiguration());

        job.setJarByClass(LRD.class);
        job.setMapperClass(OutlierMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(OutlierReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }
}
