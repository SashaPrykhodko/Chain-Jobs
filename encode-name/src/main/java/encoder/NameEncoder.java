package encoder;

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

import javax.print.DocFlavor;
import java.io.IOException;
import java.util.Base64;

public class NameEncoder {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private static final Text myKey =  new Text("");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("Encoder Mapper ");
            context.write(myKey, value);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        String name = null;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            System.out.println("Encode Reducer");

            for (Text text : values) {
                System.out.println(text);
                JSONObject jsonObject = new JSONObject(text.toString());
                System.out.println(jsonObject.toString());
                name = jsonObject.getString("name");
                System.out.println(name);
                String encodedName = Base64.getEncoder().encodeToString(name.getBytes());
                System.out.println(encodedName);
                context.write(key, new Text(encodedName.trim()));
            }
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
        job.setJarByClass(NameEncoder.class);

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
