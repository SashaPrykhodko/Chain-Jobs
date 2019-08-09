package formatter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JsonOutputFormat {

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            context.write(new Text("<configuration>"), null);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            context.write(new Text("</configuration>"), null);
        }

        private Text outputKey = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) {
//            for (Text value:values) {
//                outputKey.set();
//            }
        }
    }
}
