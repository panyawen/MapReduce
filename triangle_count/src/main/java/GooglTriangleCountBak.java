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

import java.io.IOException;
import java.util.regex.Pattern;

public class GooglTriangleCountBak {
    public static class GoogleTriangleCountMapper extends Mapper<Object, Text, Text,Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            String[] nodes = value.toString().split("\t");
            String left = nodes[0];
            context.write(new Text(left), new Text(nodes[1]));

            String[] friends = nodes[1].split(",");
            int len = friends.length;
            for (int i = 0; i < len - 1; i ++){
                String subfriend = "";
                for (int j = i + 1; j < len - 1; j++) {
                    subfriend += friends[j] + ",";
                }
                subfriend += friends[len - 1];

                context.write(new Text(friends[i] + "*"), new Text(subfriend));
            }
        }
    }

    public static class GoogleTriangleCountReducer extends Reducer<Text, Text, Text, IntWritable>{
        int answer = 0;
        String[] needCheck;
        String[] friends;
        String current = "";
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            if (key.toString().endsWith("*")){
                if (!key.toString().substring(0, key.toString().indexOf("*")).equals(current)){
                    return ;
                }

                for (Text value : values){
                    needCheck = value.toString().split(",");
                    int len1 = needCheck.length;
                    int len2 = friends.length;
                    int i = 0;
                    int j = 0;
                    while (i < len1 && j < len2){
                        int cmp = needCheck[i].compareTo(friends[j]);
                        if (cmp == 0){
                            answer ++;
                            i ++;
                            j ++;
                        }else if (cmp < 0)
                            i ++;
                        else
                            j ++;
                    }

//                    needCheck = value.toString().split(",");
//                    int len1 = needCheck.length;
//                    int len2 = friends.length;
//                    for (int i = 0; i < len1; i++)
//                        for (int j = 0; j < len2; j++)
//                            if (needCheck[i].equals(friends[j])) {
//                                answer++;
//                                break;
//                            }
                }
            }else{
                for (Text value : values)
                    friends = value.toString().split(",");
                current = key.toString();
            }
        }

        public void cleanup(Context context)
                throws IOException, InterruptedException{
            context.write(new Text("Triangle's number: "), new IntWritable(answer));
        }
    }

    public static void main(String args[]){
        try {
            if (args.length < 4){
                System.out.println("Please input inputPath, tmpdir1, tmpdir2, outputPath!");
                return ;
            }
            Configuration conf = new Configuration();
            conf.setLong("mapred.min.split.size", 2621440);
            conf.setLong("mapred.max.split.size", 2621440);
            FileSystem hdfs = FileSystem.get(conf);
            Job job = new Job(conf, "GoogleTriangleCount");
            job.setJarByClass(GoogleTriangleCount.class);
            job.setMapperClass(GoogleTriangleCountMapper.class);
            job.setReducerClass(GoogleTriangleCountReducer.class);
            job.setNumReduceTasks(5);

            Path inputPath = new Path(args[2]);
            FileStatus[] files = hdfs.listStatus(inputPath);
            for (int i=0; i<files.length; i++) {
//                System.out.println("inputpath:" + files[i].toString());
                if (Pattern.matches(new String(".*part-r-.*"), files[i].getPath().toString())) {
                    FileInputFormat.addInputPath(job, files[i].getPath());
//                    System.out.println("filter success");
                }
            }

            Path outputPath = new Path(args[3]);
            if (hdfs.exists(outputPath))
                hdfs.delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
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
