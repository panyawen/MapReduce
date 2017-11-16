import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

public class GoogleGraphBuild {
    public static class GraphBuildMapper extends Mapper<Object, Text, Text, IntWritable> {
//        long max = 0;
//        Map<String> nodeMap = new Map<String>();
        public void setup(Context context){
            System.out.println("GraphBuildMapper setup()");
        }
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            String nodes[] = value.toString().split("\t");
            if(nodes.length < 2)
                return;
//            long node1 = Long.parseLong(nodes[0]);
//            long node2 = Long.parseLong(nodes[1]);
//            if (node1 > max)
//                max = node1;
//            if (node2 > max)
//                max = node2;

//            if (nodes[0].equals(nodes[1]))
//                return ;

//            int len1 = nodes[0].length();
//            int len2 = nodes[1].length();
//            for (int i = 0; i < 15 - len1; i ++)
//                nodes[0] = "0" + nodes[0];
//            for (int i = 0; i < 15 - len2; i ++)
//                nodes[1] = "0" + nodes[1];

            if (nodes[0].compareTo(nodes[1]) <= 0) {
                context.write(new Text(nodes[0] + "#" + nodes[1]), new IntWritable(1));
            }else{
                context.write(new Text(nodes[1] + "#" + nodes[0]), new IntWritable(1));
            }
        }

        public void cleanup(Context context)
                throws IOException, InterruptedException{
//            System.out.println("the largest number:" + max);
        }
    }

    public static class GraphBuildPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTask){
            String[] nodes = key.toString().split("#");
            return super.getPartition(new Text(nodes[0]), value, numReduceTask);
        }
    }

    public static class GraphBuildReducer extends Reducer<Text, IntWritable, Text, Text>{
        String currentItem = " ";
        String friends = " ";
        public void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException{
            String[] nodes = key.toString().split("#");
            String node1 = nodes[0];
            String node2 = nodes[1];

            System.out.println(nodes[0] + " " + nodes[1]);

            if (!node1.equals(currentItem) && !currentItem.equals(" ")){
                context.write(new Text(currentItem), new Text(friends));
                friends = " ";
            }
            currentItem = node1;
            if (friends.equals(" "))
                friends = node2;
            else
                friends += "," + node2;
        }
        public void cleanup(Context context)
                throws IOException, InterruptedException{
            if (!friends.equals(" "))
                context.write(new Text(currentItem), new Text(friends));
        }
    }

    public static void main(String args[]){
        if (args.length < 4){
            System.out.println("Please input inputPath, tmpdir1, tmpdir2, outputPath!");
            return ;
        }
        try {
            System.out.println("Start GraphBuild");

            Configuration conf = new Configuration();
            conf.setLong("mapred.min.split.size", 10485760);
            conf.setLong("mapred.max.split.size", 10485760);
            Job graphBuildJob = new Job(conf, "GoogleGraphBuild");
            graphBuildJob.setJarByClass(GoogleGraphBuild.class);
            graphBuildJob.setMapperClass(GraphBuildMapper.class);

            System.out.println("Set GraphBuildMapper");

            graphBuildJob.setPartitionerClass(GraphBuildPartitioner.class);
            graphBuildJob.setReducerClass(GraphBuildReducer.class);
            graphBuildJob.setNumReduceTasks(30);
            graphBuildJob.setMapOutputKeyClass(Text.class);
            graphBuildJob.setMapOutputValueClass(IntWritable.class);

//            FileInputFormat.addInputPath(graphBuildJob, new Path(args[1]));

            Path inputPath = new Path(args[1]);
            FileSystem hdfs = FileSystem.get(conf);
            FileStatus[] files = hdfs.listStatus(inputPath);
            for (int i=0; i<files.length; i++) {
//                System.out.println("inputpath:" + files[i].toString());
                if (Pattern.matches(new String(".*part-r-.*"), files[i].getPath().toString())) {
                    FileInputFormat.addInputPath(graphBuildJob, files[i].getPath());
//                    System.out.println("filter success");
                }
            }

            Path outputPath = new Path(args[2]);
            if (hdfs.exists(outputPath))
                hdfs.delete(outputPath, true);
            FileOutputFormat.setOutputPath(graphBuildJob, outputPath);
            if (!graphBuildJob.waitForCompletion(true)){
                System.exit(1);
            }
        }catch (IOException e){
            e.printStackTrace();
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}
