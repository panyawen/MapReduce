import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.net.URISyntaxException;

import java.io.IOException;

import java.util.*;

public class InvertedIndexSort {
    public static class InvertedIndexMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        protected void map(Object key, Text value, Context context)
                throws  IOException, InterruptedException{
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String temp = new String();
            String line = value.toString().toLowerCase();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()){
                temp = itr.nextToken();
                Text word = new Text();
                word.set(temp + "#" + fileName);
                context.write(word, new IntWritable(1));
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

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp = new String();
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();
        DoubleWritable avg = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            word1.set(key.toString().split("#")[0]);
            temp = key.toString().split("#")[1];
            for (IntWritable val : values){
                sum += val.get();
            }
            word2.set(temp + ":" + sum);
            if (!CurrentItem.equals(word1) && !(CurrentItem.toString().equals(" "))){

                //  StringBuilder out = new StringBuilder();
                long count = 0;
                int len = postingList.size();
                if(len > 0) {
                    for (int i = 0; i < len; i++) {
                        String p = postingList.get(i);
                        count += Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
                    }

                    double d = count * 1.0 / len;
                    avg.set(d);

                    //out.append("" + count * 1.0 / len);   //average count
                    if (count > 0)
                        context.write(CurrentItem, avg);
                    postingList = new ArrayList<String>();
                }
            }
            CurrentItem = new Text(word1);
            postingList.add(word2.toString());
        }
        public void cleanup(Context context)
                throws IOException, InterruptedException{
            long count = 0;
            int len = postingList.size();
            if(len > 0) {
                for (int i = 0; i < len; i++) {
                    String  p = postingList.get(i);
                    count += Long.parseLong(p.substring(p.indexOf(":") + 1, p.length()));
                }
                double d = count * 1.0 / len;
                avg.set(d);
                if (count > 0)
                    context.write(CurrentItem, avg);
            }
        }
    }

    public static void main(String args[]) throws Exception{
        String tmpDir = "tmp-dir";
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        try {
            String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2){
                System.err.println("Usage: inputDir outputDir");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "InvertedIndex");
            job.setJarByClass(InvertedIndexSort.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(SumCombiner.class);
            job.setPartitionerClass(NewPartitioner.class);
            job.setReducerClass(InvertedIndexReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);


            Path tmpPath = new Path(tmpDir);
            if(hdfs.exists(tmpPath))
                hdfs.delete(tmpPath, true);
            FileOutputFormat.setOutputPath(job, tmpPath);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            for (int i = 0; i < otherArgs.length - 1; ++i) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }
            FileOutputFormat.setOutputPath(job,
                    new Path(tmpDir));
            if (job.waitForCompletion(true)){
                Job sortJob = new Job(conf, "sort");
                sortJob.setJarByClass(InvertedIndexSort.class);
                FileInputFormat.addInputPath(sortJob, new Path(tmpDir));
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);
                sortJob.setMapperClass(InverseMapper.class);
//                sortJob.setNumReduceTasks(1);   //change number of reduceTask

                Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
                if (hdfs.exists(outputPath))
                    hdfs.delete(outputPath, true);
                FileOutputFormat.setOutputPath(sortJob, outputPath);

                sortJob.setMapOutputKeyClass(DoubleWritable.class);
                sortJob.setMapOutputValueClass(Text.class);

                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
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
