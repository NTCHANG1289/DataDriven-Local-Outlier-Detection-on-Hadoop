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
public class SecondK_DIS {

    public static class OutlierMapper extends Mapper<LongWritable, Text, Text, Text> {

        private List<String> inform_file = new ArrayList<String>();
        private HashMap<String,String> zone_profile = new HashMap<>();
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
                int lens= 0;
                for (int i=0; i<line.length(); i++){
                    if (line.charAt(i)!=','){
                        lens++;
                    }else{
                        break;
                    }
                }
                zone_profile.put(line.substring(0,lens), line.substring(lens+1));
            }

        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] data= value.toString().split("__");

            String data_key = data[0]+"__"+data[1]+"__"+data[3]+"__"+data[4]+"__"+data[5];

            if (data.length!=6){
                String[] SupportZones = data[6].split("--");
                for (String SupportZone: SupportZones){
                    String[] points = zone_profile.get("Zone"+SupportZone).split("--");
                    for (String point: points){
                        context.write(new Text(data_key), new Text(point+""));
                    }
                }
                //context.write(new Text("ff"+String.valueOf(data.length)+data_key), new Text(data_value));

            }else{
                context.write(new Text(data_key), new Text(""));
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

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] data = key.toString().split("__");

            String flg = data[0];
            String point = data[1].substring(0, data[1].length()-1);
            String myKdis = data[2];
            String neighbors = data[3];
            //String nei_Kdis = data[4];

            int flag = 0;
            int Nreach = neighbors.split("--").length;

            double p_x=0;
            double p_y=0;
            List<Double> dist_table = new ArrayList<>();
            // Only prepare for flag2 case (looking in support zones)
            if (flg.equals("2")) {

                p_x = Double.parseDouble(point.split(",")[0]);
                p_y = Double.parseDouble(point.split(",")[1]);

                String[] my_nei = neighbors.split("--");
                for (String nei : my_nei) {
                    double n_x = Double.parseDouble(nei.split(",")[0]);
                    double n_y = Double.parseDouble(nei.split(",")[1]);
                    double distnei = distab(p_x, p_y, n_x, n_y);
                    dist_table.add(distnei);
                }
            }
            // looking for candidate in support zones
            for (Text value: values){

                if (flg.equals("1") || value.toString().length()==0){

                    //no change for flag1

                }else if(flg.equals("2")){

                    String Support_point = value.toString();

                    double Kdis = Double.parseDouble(myKdis);
                    double sp_x = Double.parseDouble(Support_point.split(",")[0]);
                    double sp_y = Double.parseDouble(Support_point.split(",")[1]);
                    double dist = distab(p_x,p_y,sp_x,sp_y);
                    if (dist<=Kdis){
                        flag=1;
                        //neighbors = neighbors + Support_point + "--";
                        dist_table.add(dist);
                    }
                }
            }

            if (flag==0){
                // no new neighbor added in support zones
                context.write(new Text(point), new Text("__"+myKdis));
            }else{
                //String[] new_neighbors_unpick = neighbors.split("--");
                double[] new_dist_table = new double[dist_table.size()];
                for (int i=0; i<dist_table.size();i++) { new_dist_table[i]=dist_table.get(i); }
                int[] new_nei_dist_sort_idx = sortindex(new_dist_table);

                int count = Nreach;
                int cur_idex = 0;
                int next_idex = 0;
                for (int j = Nreach-1; j < new_nei_dist_sort_idx.length - 1; j++) {
                    cur_idex = new_nei_dist_sort_idx[j];
                    next_idex = new_nei_dist_sort_idx[j + 1];
                    if (new_dist_table[next_idex] != new_dist_table[cur_idex]) {
                        break;
                    } else {
                        count++;
                    }
                }

//                String new_neighbors = "";
//                for (int i=0; i<count; i++){
//                    int idx = new_nei_dist_sort_idx[i];
//                    new_neighbors = new_neighbors + new_neighbors_unpick[idx] + "--";
//                }
                int idx = new_nei_dist_sort_idx[count-1];
                context.write(new Text(point), new Text("__" + dist_table.get(idx)));
            }
        }
    }


    public static void main(String[] args) throws Exception{
        String inputPath = "./out_K_DIS/";
        String outputPath = "./out_2nd_K_DIS/";
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = new Job(conf, "SecondK_DIS");


        String path1 = "./out_AssignZone/part-r-00000";
        DistributedCache.addCacheFile(new Path(path1).toUri(), job.getConfiguration());

        job.setJarByClass(SecondK_DIS.class);
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
