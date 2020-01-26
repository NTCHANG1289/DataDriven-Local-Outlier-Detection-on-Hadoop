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

//
public class K_DIS {

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
        public int countSupportStep(double K_dis, double dist2edge, double step){
            double vec= K_dis-Math.abs(dist2edge);
            int count =0;
            while ( vec >= 0){
                vec-=step;
                count++;
            }
            return count;
        }

        private List<String> inform_file = new ArrayList<String>();
        public void setup(Reducer.Context context) {
            try {
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

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] x_inform = inform_file.get(0).split(",");
            String[] y_inform = inform_file.get(0).split(",");
            int n = Integer.parseInt(inform_file.get(1));
            int k = Integer.parseInt(inform_file.get(2));
            double x_min = Double.parseDouble(x_inform[0]);
            double x_max = Double.parseDouble(x_inform[1]);
            double y_min = Double.parseDouble(y_inform[0]);
            double y_max = Double.parseDouble(y_inform[1]);

            List<String> all_points= new ArrayList<>();

            // collect all data in one area
            for (Text value : values) {
                all_points.add(value.toString());
            }

            String flag = "";
            if (all_points.size()==0) {
                //
            } else if(all_points.size()<k){
                //flag3
                flag = "2";
                String all_zone = "";
                for (int i=0; i<n*n; i++){
                    all_zone += String.valueOf(i);
                    all_zone += "--";
                }
                for (int i = 0; i < all_points.size(); i++) {
                    context.write(new Text(flag + "__" + all_points.get(i)), new Text("__"+ key + "__"+String.valueOf(distab(x_max,y_max,0,0))+ "__" + "--"+ "__" + "--" + "__"+ all_zone));
                }

            } else {

                // distance table
                double[][] all_dists = new double[all_points.size()][all_points.size()];
                for (int i = 0; i < all_points.size(); i++) {
                    String p1 = all_points.get(i);
                    double[] xy1 = StringsToDoubles(p1.split(","));
                    for (int j = 0; j < all_points.size(); j++) {
                        String p2 = all_points.get(j);
                        double[] xy2 = StringsToDoubles(p2.split(","));
                        double tmp = 0;
                        for (int dim = 0; dim < xy1.length; dim++) {
                            tmp = tmp + (xy1[dim] - xy2[dim]) * (xy1[dim] - xy2[dim]);
                        }
                        all_dists[i][j] = Math.sqrt(tmp);
                    }
                }


                // sorted index table
                int[][] dists_sort = new int[all_points.size()][all_points.size()];
                for (int i = 0; i < all_points.size(); i++) {
                    int[] tmp = sortindex(all_dists[i]);
                    for (int j = 0; j < all_points.size(); j++) {
                        dists_sort[i][j] = tmp[j];
                    }
                }

                //num of neighbors and K-distance table
                int[] Nreach = new int[all_points.size()];
                double[] Kdis = new double[all_points.size()];
                for (int i = 0; i < all_points.size(); i++) {
                    int count = k;
                    int cur_idex = 0;
                    int next_idex = 0;
                    for (int j = k; j < all_points.size() - 1; j++) {
                        cur_idex = dists_sort[i][j];
                        next_idex = dists_sort[i][j + 1];
                        if (all_dists[i][next_idex] != all_dists[i][cur_idex]) {
                            break;
                        } else {
                            count++;
                        }
                    }
                    Kdis[i] = all_dists[i][cur_idex];
                    Nreach[i] = count;
                }


                //flag 2
                int zone = Integer.parseInt(key.toString().substring(4));
                String extend_zone = "";
                int x_zone = zone % n;
                int y_zone = zone / n;
                double x_step = (x_max-x_min)/n;
                double y_step = (y_max-y_min)/n;
                double l = x_zone * (x_step) + x_min;
                double r = x_zone * (x_step) + x_step + x_min;
                double b = y_zone * (y_step) + y_min;
                double t = y_zone * (y_step) + y_step + y_min;

//                int count_l = 0;
//                int count_r = 0;
//                int count_b = 0;
//                int count_t = 0;

                for (int i = 0; i < all_points.size(); i++) {
                    String[] p1 = all_points.get(i).split(",");
                    double px = Double.parseDouble(p1[0]);
                    double py = Double.parseDouble(p1[1]);

                    // check dis(p, grid) left, right, bottom, top
                    if ((px-l)<Kdis[i] || (r-px)<=Kdis[i] || (py-b)<Kdis[i] || (t-py)<=Kdis[i]){
                        flag = "2";

                        int count_l = countSupportStep(Kdis[i], px-l, x_step);
                        int count_r = countSupportStep(Kdis[i], px-r, x_step);
                        int count_b = countSupportStep(Kdis[i], py-b, y_step);
                        int count_t = countSupportStep(Kdis[i], py-t, y_step);
//                        count_l = countSupportStep(Kdis[i], px-l, x_step);
//                        count_r = countSupportStep(Kdis[i], px-r, x_step);
//                        count_b = countSupportStep(Kdis[i], py-b, y_step);
//                        count_t = countSupportStep(Kdis[i], py-t, y_step);
//                        if (px==17 && py==14){
//                            System.out.println(Kdis[i]);
//                            System.out.println(count_l);
//                            System.out.println(count_r);
//                            System.out.println(count_b);
//                            System.out.println(count_t);
//                        }
                        List<Integer> ext_zones = new ArrayList<>();
                        int myZone = Integer.parseInt(key.toString().substring(4));
                        int exZone;

                        // check for left/top
                        if(count_l>0 && count_t>0  && distab(px, py, l, t)<Kdis[i]){
                            if ((l-x_step)>=x_min && (t+y_step)< y_max){
                                exZone = myZone-1;
                                exZone += n;
                                ext_zones.add(exZone);
                            }
                        }
                        // check for left/bottom
                        if(count_l>0 && count_b>0  && distab(px, py, l, b)<Kdis[i]){
                            if ((l-x_step)>=x_min && (y_step-b)>=y_min){
                                exZone = myZone-1;
                                exZone -= n;
                                ext_zones.add(exZone);
                            }
                        }
                        // check for right/top
                        if(count_r>0 && count_t>0  && distab(px, py, r, t)<=Kdis[i]){
                            if ((x_step+r)<=x_max && (t+y_step)< y_max){
                                exZone = myZone+1;
                                exZone += n;
                                ext_zones.add(exZone);
                            }
                        }
                        // check for right/bottom
                        if(count_r>0 && count_b>0  && distab(px, py, r, b)<Kdis[i]){
                            if ((x_step+r)<=x_max && (y_step-b)>= y_min){
                                exZone = myZone+1;
                                exZone -= n;
                                ext_zones.add(exZone);
                            }
                        }
//
                        // check for left
                        while (count_l>0){
                            exZone = myZone-count_l;
                            if ((l-count_l*x_step)>=x_min) {
                                ext_zones.add(exZone);
                            }
                            count_l--;
                        }
                        // check for right
                        while (count_r>0){
                            exZone = myZone+count_r;
                            if ((r+count_r*x_step)<=x_max) {
                                ext_zones.add(exZone);
                            }
                            count_r--;
                        }
                        // check for bottom
                        while (count_b>0){
                            exZone = myZone-count_b*n;
                            if ((b-count_b*y_step)>=y_min) {
                                ext_zones.add(exZone);
                            }
                            count_b--;
                        }
                        // check for top
                        while (count_t>0){
                            exZone = myZone+count_t*n;
                            if ((t+count_t*y_step)<=y_max) {
                                ext_zones.add(exZone);
                            }
                            count_t--;
                        }

                        extend_zone = "";
                        for (int z: ext_zones){
                            if (z>=0){
                                extend_zone += String.valueOf(z);
                                extend_zone += "--";
                            }
                        }
                        if (extend_zone.equals("")){flag = "1";}

                    }


                    else {
                        flag = "1";
                    }

                    String nei_list = "";
                    String neiKdis_list = "";
                    for (int j = 1; j <= Nreach[i]; j++) {
                        int nei_idx = dists_sort[i][j];
                        nei_list += String.valueOf(all_points.get(nei_idx));
                        nei_list += "--";
                    }
                    for (int j = 1; j <= Nreach[i]; j++) {
                        int nei_idx = dists_sort[i][j];
                        neiKdis_list += String.valueOf(Kdis[nei_idx]);
                        neiKdis_list += "--";
                    }
                    context.write(new Text(flag + "__" + all_points.get(i)), new Text("__"+ key + "__"+String.valueOf(Kdis[i])+ "__" + nei_list + "__" + neiKdis_list + "__"+ extend_zone));
                    }

            }

        }
    }


    public static void main(String[] args) throws Exception{
        String inputPath = "./input/datasetP.csv";
        String outputPath = "./out_K_DIS/";
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = new Job(conf, "K_DIS");


        String path1 = "./input/inform.csv";
        DistributedCache.addCacheFile(new Path(path1).toUri(), job.getConfiguration());

        job.setJarByClass(K_DIS.class);
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
