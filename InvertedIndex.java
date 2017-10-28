import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

//import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.net.URISyntaxException;

import java.io.IOException;

import java.util.*;

public class InvertedIndex2 {
    public static class InvertedIndexMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        protected void map(Object key, Text value, Context context)
                throws  IOException, InterruptedException{
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            fileName = fileName.substring(0, fileName.length() - 14);
            String temp = new String();
            String line = value.toString().toLowerCase();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()){
                temp = itr.nextToken();
//                if (!stopword.contains(temp)){
                Text word = new Text();
                word.set(temp + "#" + fileName);
                context.write(word, new IntWritable(1));
//                }
            }
        }

    }

    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public  static class NewPartitioner extends HashPartitioner<Text, IntWritable>{
        public int getPartition(Text key, IntWritable value, int numReduceTasks){
            String term = new String();
            //     term = key.toString().split(",")[0]怀; // "#" or ","
            term = key.toString().split("#")[0];
            //  System.out.println();
            //System.out.println("Key:" + term);
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text>{
        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp = new String();
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;

            Configuration conf = context.getConfiguration();
            int test = conf.getInt("test", 1);

            word1.set(key.toString().split("#")[0]);
            temp = key.toString().split("#")[1];
            for (IntWritable val : values){
                sum += val.get();
            }
            word2.set(temp + ":" + sum);
            if (!CurrentItem.equals(word1) && !(CurrentItem.toString().equals(" "))){

                StringBuilder out = new StringBuilder();
                long count = 0;
                int len = postingList.size();
                if(len > 0) {
                    String p = postingList.get(0);
                    out.append(p);
                    count += Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
                    for (int i = 1; i < len; i++) {
                        out.append(";");
                        p = postingList.get(i);
                        out.append(p);
                        count += Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
                    }

                    out.insert(0, "" + count * 1.0 / len + ",");   //average count
                    if (count > 0)
                        context.write(CurrentItem, new Text(out.toString()));
                    postingList = new ArrayList<String>();
                }
            }
            CurrentItem = new Text(word1);
            postingList.add(word2.toString());
        }

        public void cleanup(Context context)
                throws IOException, InterruptedException{
            StringBuilder out = new StringBuilder();
            long count = 0;
            int len = postingList.size();
            if(len > 0) {
                String p = postingList.get(0);
                out.append(p);
                count += Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
                for (int i = 1; i < len; i++) {
                    out.append(";");
                    p = postingList.get(i);
                    out.append(p);
                    count += Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
                }
                out.insert(0, "" + count * 1.0 / len + ",");
                if (count > 0)
                    context.write(CurrentItem, new Text(out.toString()));
            }
        }
    }

    public static void main(String args[]){
        try {
            Configuration conf = new Configuration();

            conf.setInt("test", 111);

            FileSystem hdfs = FileSystem.get(conf);

            String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2){
                System.err.println("Usage: inputDir outputDir");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "InvertedIndex");
            job.setJarByClass(InvertedIndex2.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(SumCombiner.class);
            job.setPartitionerClass(NewPartitioner.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            for (int i = 0; i < otherArgs.length - 1; ++i) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }

            Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
            if (hdfs.exists(outputPath))
                hdfs.delete(outputPath, true);

            FileOutputFormat.setOutputPath(job,
                    outputPath);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (IOException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }

    }
}
