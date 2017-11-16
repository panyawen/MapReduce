import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GoogleGetEdge {
    public static class GoogleGetEdgeMapper extends Mapper<Object, Text, Text, IntWritable>{
        int cmp;
        long max = 0;
        public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException{
            String[] nodes =  value.toString().split(" ");
            int len1 = nodes[0].length();
            int len2 = nodes[1].length();

            if (len1 > max)
                max = len1;
            if (len2 > max)
                max = len2;

            for (int i = 0; i < 30 - len1; i ++)
                nodes[0] = "0" + nodes[0];
            for (int i = 0; i < 30 - len2; i ++)
                nodes[1] = "0" + nodes[1];
            cmp = nodes[0].compareTo(nodes[1]);
            if (cmp < 0)
                context.write(new Text(nodes[0] + "#" + nodes[1]), new IntWritable(-1));
            else if((cmp > 0))
                context.write(new Text(nodes[1] + "#" + nodes[0]), new IntWritable(1));
        }

        public void cleanup(Context context){
            System.out.println("The max num_length is:" + max);
        }
    }

    public static class GoogleGetEdgeReducer extends Reducer<Text, IntWritable, Text, Text>{
        int belowZero;
        int aboveZero;
        String[] nodes;
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException{
            belowZero = 0;
            aboveZero = 0;
            for (IntWritable value : values){
                if (value.get() == 1)
                    aboveZero ++;
                else
                    belowZero ++;
            }
            if (aboveZero > 0 && belowZero > 0) {
                nodes = key.toString().split("#");
                context.write(new Text(nodes[0]), new Text(nodes[1]));
            }
        }
    }

    public static void main(String[] args){
        if (args.length < 4){
            System.out.println("Please input inputPath, tmpPath1, tmpPath2, outputPath!");
            return ;
        }
        try {
            Configuration conf = new Configuration();
            conf.setLong("mapred.min.split.size", 10485760);
            conf.setLong("mapred.max.split.size", 10485760);
            Job job = new Job(conf, "GoogleGetEdge");
            job.setJarByClass(GoogleGetEdge.class);
            job.setMapperClass(GoogleGetEdgeMapper.class);
            job.setReducerClass(GoogleGetEdgeReducer.class);
            job.setNumReduceTasks(50);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            FileSystem hdfs = FileSystem.get(conf);
            Path outputPath = new Path(args[1]);
            if (hdfs.exists(outputPath))
                hdfs.delete(outputPath, true);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, outputPath);

            if (!job.waitForCompletion(true)){
                System.exit(1);
            }
        }catch (IOException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }
}
