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

import java.io.*;
import java.security.Key;
import java.util.*;
import java.util.Comparator;


//Assign points to different Zones
public class AssignZone {

    public static class OutlierMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        private List<String> inform_file = new ArrayList<String>();
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
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] x_inform = inform_file.get(0).split(",");
            String[] y_inform = inform_file.get(0).split(",");
            int n = Integer.parseInt(inform_file.get(1));
            double x_min = Double.parseDouble(x_inform[0]);
            double x_max = Double.parseDouble(x_inform[1]);
            double y_min = Double.parseDouble(y_inform[0]);
            double y_max = Double.parseDouble(y_inform[1]);

            String[] data = value.toString().split(",");
            double p_x = Double.parseDouble(data[0]);
            double p_y = Double.parseDouble(data[1]);

            double x = x_max-x_min;
            double y = y_max-y_min;
            double l, r, t, b;
            String zone;
            int count = 0;
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    l = j * x / n + x_min;
                    r = x / n + j * x / n + x_min;
                    t = y / n + i * y / n + y_min;
                    b = i * y / n + y_min;
                    zone = String.valueOf(count);
                    if (p_y >= b && p_y < t && p_x >= l && p_x < r) {
                        context.write(new Text("Zone"+zone), new Text(p_x+","+p_y));
                    }
                    count++;
                }
            }
        }
    }

    public static class OutlierReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> all_points= new ArrayList<>();

            // collect all data in one area
            for (Text value : values) {
                all_points.add(value.toString());
            }

            String all_points_list = "";
            for (int i=0; i<all_points.size(); i++){
                all_points_list+=all_points.get(i);
                all_points_list+="--";
            }

            context.write(new Text(key), new Text(all_points_list));

        }
    }


    public static void main(String[] args) throws Exception{
        String inputPath = "./input/datasetP.csv";
        String outputPath = "./out_AssignZone/";
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = new Job(conf, "AssignZone");


        String path1 = "./input/inform.csv";
        DistributedCache.addCacheFile(new Path(path1).toUri(), job.getConfiguration());

        job.setJarByClass(AssignZone.class);
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
