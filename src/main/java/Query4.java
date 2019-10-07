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

import java.io.*;
import java.util.ArrayList;
import java.util.List;
// The code is for Distance-Based Outlier Detection Clustering. If a point with lesss than k points that are within r distance,
// then report the point as an outlier.
// The setUp part is used to define the r - radius and k - counts.
// The mapper split the dataset into n*n blocks, and distinguish the zone or supporting area of points.
// The reducer then start calculating the distances between points and report outliers in each zone.
// The code is worked by Nai-tan Chang.
public class Query4 {
    public static class OutlierMapper extends Mapper<LongWritable, Text, Text, Text> {

        private List<String> rk_file = new ArrayList<String>();
        public void setup(Context context) {
            try{
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    rk_file.clear();
                    try {
                        while ((line = cacheReader.readLine()) != null) {
                            rk_file.add(line);
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
            String line = rk_file.get(0);
            String[] rk = line.split(",");
            double radius = Double.parseDouble(rk[0]);
            int k = Integer.parseInt(rk[1]);

            String[] data = value.toString().split(",");
            int dat_x = Integer.parseInt(data[0]);
            int dat_y = Integer.parseInt(data[1]);
            int x = 10000;
            int y = 10000;
            int l, r, t, b;
            int n = 10;
            String zone;
            int count = 0;
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    l = j * x / n;
                    r = x / n + j * x / n;
                    t = y / n + i * y / n;
                    b = i * y / n;
                    zone = "Zone" + String.valueOf(count);
                    if (dat_y > b - radius && dat_y <= t + radius && dat_x > l - radius && dat_x <= r + radius) {
                        if (dat_y > b && dat_y <= t && dat_x > l && dat_x <= r) {
                            context.write(new Text(zone), new Text(data[0] + "," + data[1] + ",C"));
                        } else {
                            context.write(new Text(zone), new Text(data[0] + "," + data[1] + ",S"));
                        }
                    }
                    count++;
                }
            }
        }
    }

    public static class OutlierReducer extends Reducer<Text,Text,Text,Text> {
        private List<String> rk_file = new ArrayList<String>();
        public void setup(Reducer.Context context) {
            try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    rk_file.clear();
                    try {
                        while ((line = cacheReader.readLine()) != null) {
                            rk_file.add(line);
                        }
                    } finally {
                        cacheReader.close();
                    }
                }
            } catch (IOException e) {
                System.out.println(e);
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line = rk_file.get(0);
            String[] rk = line.split(",");
            double radius = Double.parseDouble(rk[0]);
            int k = Integer.parseInt(rk[1]);

            int count = 0;
            List<String> c_points= new ArrayList<>();
            List<String> all_points= new ArrayList<>();

            for (Text value : values) {
                String[] data = value.toString().split(",");
                String cs = data[2];
                all_points.add(data[0]+","+data[1]);
                if(cs.equals("C")){
                    c_points.add(data[0]+","+data[1]);
                }
            }
            for (String c : c_points) {
                String[] c_data = c.toString().split(",");
                double c_x = Double.parseDouble(c_data[0]);
                double c_y = Double.parseDouble(c_data[1]);
                count = 0;
                for (String all: all_points){
                    String[] all_data = all.toString().split(",");
                    double all_x = Double.parseDouble(all_data[0]);
                    double all_y = Double.parseDouble(all_data[1]);
                    double dis = Math.sqrt(Math.pow(all_x - c_x,2)+Math.pow(all_y - c_y,2));
                    if (dis<=radius){
                        count++;
                    }
                }
                if (count-1 < k ){
                    context.write(new Text(c_data[0]+","+c_data[1]+","+String.valueOf(count-1)),key);
                }
            }
        }
    }


    public static void main(String[] args) throws Exception{
        String inputPath = args[0]+"datasetP.csv";
        String outputPath = args[1];
        String radius = args[2];
        String threshold = args[3];
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = new Job(conf, "Query4");


        String path1 = "./input/r_k.csv";
        File file1 = new File(path1);
        FileWriter fwPoint = new FileWriter(file1.getAbsoluteFile());
        BufferedWriter bwPoint = new BufferedWriter(fwPoint);
        bwPoint.write(radius+","+threshold);
        bwPoint.flush();
        fwPoint.close();

        DistributedCache.addCacheFile(new Path(path1).toUri(), job.getConfiguration());

        job.setJarByClass(Query4.class);
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