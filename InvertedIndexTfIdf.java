import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class InvertedIndexTfIdf {
    public static class WordDocMapper
            extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            char[] fn = fileName.toCharArray();
            String author;
            int i;
            for(i=0; i<fileName.length(); i++)
                if(fn[i] >= '0' && fn[i] <= '9')
                    break;
            author = fileName.substring(0, i);
            String temp = new String();
            String line = value.toString().toLowerCase();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                temp = itr.nextToken();
                Text word = new Text();
                word.set(temp + "#" + fileName);
                Text v = new Text();
                v.set(author + "#1");
                context.write(word, v);
            }
        }
    }

    public static class WordDocCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            int sum = 0;
            String author = "author";
            for (Text val : values){
                String sval = val.toString();
                String scnt = sval.substring(sval.indexOf("#") + 1, sval.length());
                author = sval.substring(0, sval.indexOf("#"));
                sum += Long.parseLong(scnt);
            }
            context.write(key, new Text(author + "#" + sum));
        }
    }

    public static class NewPartitioner extends HashPartitioner<Text, Text>{
        public int getPartition(Text key, Text value, int numReduceTask){
            String term = new String();
            term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTask);
        }
    }

    public static class WordDocReducer extends Reducer<Text, Text, Text, Text>{
        private Text word1 = new Text();
        private Text word2 = new Text();
        static Text currentItem = new Text(" ");
        DecimalFormat df = new DecimalFormat("#.00");
        private int docNum = 0;
        private String author = "author";
        static List<String> list = new ArrayList<String>();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            int docSum = conf.getInt("DocSum", 1);
            int sum = 0;
            word1.set(key.toString().split("#")[0]);  //word

            for (Text val : values){
                String sval = val.toString();
                author = sval.split("#")[0];
                sum += Long.parseLong(sval.split("#")[1]);
            }
            word2.set(author + "#" + sum);

            if(!currentItem.equals(word1) && !(currentItem.toString().equals(" "))){
                StringBuilder out = new StringBuilder();
                for(String p : list){
                    out.append(p);
                    out.append(";");
                }

//                out.append("" + df.format(Math.log(docSum * 1.0 / (docNum + 1)) / Math.log(2)));   //IDF
                out.append(String.format("%.2f", (Math.log(docSum * 1.0 / (docNum + 1)) / Math.log(2))));
                context.write(currentItem, new Text(out.toString()));
                list = new ArrayList<String>();
                docNum = 0;
            }
            docNum ++;
            currentItem = new Text(word1);
            list.add(word2.toString());
        }

        public void cleanup(Context context)
                throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            int docSum = conf.getInt("DocSum",6);
            StringBuilder out = new StringBuilder();
            for(String p : list){
                out.append(p);
                out.append(";");
            }

//            double tmpd = Math.log(docSum * 1.0 / (docNum + 1)) / Math.log(2) * 100;

            out.append(String.format("%.2f", (Math.log(docSum * 1.0 / (docNum + 1)) / Math.log(2))));
            context.write(currentItem, new Text(out.toString()));
        }
    }

    public static class AuthorDocMapper extends Mapper<Text, Text, Text, Text>{
        public void map(Text key, Text values, Context context)
                throws IOException, InterruptedException{
            String line = values.toString().toLowerCase();
            String []items = line.split(";");
            int len = items.length;
            for (int i=0; i<len - 1; i++){
                String author = items[i].split("#")[0];
                String newKey = author + ", " + key.toString() + ",";
                String val = items[i].split("#")[1] + "#" + items[len - 1];
                context.write(new Text(newKey), new Text(val));
            }
        }
    }

    public static class AuthorDocPartitioner extends HashPartitioner<Text, Text>{
        public int getPartition(Text key, Text value, int numReduceTask){
            return super.getPartition(new Text(key.toString().split("#")[1]), value, numReduceTask);
        }
    }

    public static class AuthorDocCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context conetxt)
                throws IOException, InterruptedException{
            int sum = 0;
            String idf = "";
            for (Text val : values){
                sum += Long.parseLong(val.toString().split("#")[0]);
                idf = val.toString().split("#")[1];
            }
            conetxt.write(key, new Text("" + sum + "#" + idf));
        }
    }

    public static class AuthorDocReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context conetxt)
                throws IOException, InterruptedException{
            int sum = 0;
            String idf = "";
            for (Text val : values){
                sum += Long.parseLong(val.toString().split("#")[0]);
                idf = val.toString().split("#")[1];
            }
            conetxt.write(key, new Text("" + sum + ", " + idf));
        }
    }

    public static void main(String args[]){
        try {
            Configuration conf = new Configuration();
            conf.setInt("test", 1);
            FileSystem hdfs = FileSystem.get(conf);
            String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
            String tmpDir = "tmp-dir";
            if (otherArgs.length < 2){
                System.err.println("Usage: inputDir outputDir");
                System.exit(2);
            }

            int cnt = 0;
            for (int i = 0; i < otherArgs.length - 1; ++i) {
                Path inputPath = new Path(otherArgs[i]);
                cnt += hdfs.listStatus(inputPath).length;
            }
            conf.setInt("DocSum", cnt);

            Job job = Job.getInstance(conf, "WordDoc");
            for (int i = 0; i < otherArgs.length - 1; ++i) {
                Path inputPath = new Path(otherArgs[i]);
                FileInputFormat.addInputPath(job, inputPath);
                cnt += hdfs.listStatus(inputPath).length;
            }

            System.out.println("docSum:" + conf.getInt("DocSum", 1));

            Path outputPath = new Path(tmpDir);
            if (hdfs.exists(outputPath))
                hdfs.delete(outputPath, true);

            job.setJarByClass(InvertedIndexTfIdf.class);
            job.setMapperClass(WordDocMapper.class);
            job.setCombinerClass(WordDocCombiner.class);
            job.setPartitionerClass(NewPartitioner.class);
            job.setReducerClass(WordDocReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            FileOutputFormat.setOutputPath(job, outputPath);
            if (job.waitForCompletion(true)) {
                Job job2 = Job.getInstance(conf, "AuthorDoc");
                job2.setJarByClass(InvertedIndexTfIdf.class);
                job2.setMapperClass(AuthorDocMapper.class);
                job2.setReducerClass(AuthorDocReducer.class);
                job2.setCombinerClass(AuthorDocCombiner.class);
                job2.setPartitionerClass(AuthorDocPartitioner.class);
                job2.setInputFormatClass(SequenceFileInputFormat.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
//                job2.setNumReduceTasks(1);
                FileInputFormat.addInputPath(job2, new Path(tmpDir));

                Path outputPath2 = new Path(otherArgs[otherArgs.length - 1]);
                if (hdfs.exists(outputPath2))
                    hdfs.delete(outputPath2, true);
                FileOutputFormat.setOutputPath(job2, outputPath2);
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
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
