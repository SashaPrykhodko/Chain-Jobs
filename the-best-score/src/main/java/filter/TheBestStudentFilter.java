package filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class TheBestStudentFilter {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private static final Text myKey =  new Text("");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            System.out.println("in Best score Mapper");
            context.write(myKey, value);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        int maxScore;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

//            int studentScore = 0;
//            JSONObject jsonObject = null;
//
//            System.out.println("List of Student (Teh Best Score");
//            for (Text text:values) {
//                System.out.println(text.toString());
//
//                jsonObject = new JSONObject(text.toString());
//                studentScore = jsonObject.getInt("middle-mark");
//                if (studentScore >= maxScore) {
//                    maxScore = studentScore;
//                }
//            }

            Iterator iter = values.iterator();
            System.out.println(iter.toString());

            System.out.println("Using iterator");
            while (iter.hasNext()){
                System.out.println(iter.next().toString());
            }

            Iterator iter2 = values.iterator();
            System.out.println(iter2.toString());

            System.out.println("Using iterator        2");
            while (iter2.hasNext()){
                System.out.println(iter2.next().toString());
            }
//
//            if (studentScore == maxScore) {
//                System.out.println("max score = " + maxScore);
//                jsonObject.remove("class");
//                jsonObject.remove("middle-mark");
//                context.write(key, new Text(jsonObject.toString()));
//            }
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        runJob(args[0], args[1]);
    }

    private static void runJob(String input, String output)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TheBestStudentFilter.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}