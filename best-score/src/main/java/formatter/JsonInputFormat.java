package formatter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class JsonInputFormat extends FileInputFormat<LongWritable, MapWritable> {

    @Override
    public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        return new JsonRecordReader();
    }

    private static class JsonRecordReader extends RecordReader<LongWritable, MapWritable> {

        private LineRecordReader reader = new LineRecordReader();

        private final Text currentLine = new Text();
        private final MapWritable value = new MapWritable();
        private final JSONParser parser = new JSONParser();

        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {
            reader.initialize(inputSplit, taskAttemptContext);
        }

        public LongWritable getCurrentKey()
                throws IOException, InterruptedException {
            return reader.getCurrentKey();
        }

        public MapWritable getCurrentValue()
                throws IOException, InterruptedException {
            return value;
        }

        public float getProgress()
                throws IOException, InterruptedException {
            return reader.getProgress();
        }

        public void close()
                throws IOException {
            reader.close();
        }

        public boolean nextKeyValue()
                throws IOException, InterruptedException {
            while (reader.nextKeyValue()) {
                value.clear();
                if (decodeLineToJson(parser, reader.getCurrentValue(), value)) {
                    return true;
                }
            }
            return false;
        }

        public static boolean decodeLineToJson(JSONParser parser, Text line, MapWritable value) {

            try {
                JSONObject jsonObject = (JSONObject) parser.parse(line.toString());
                for (Object key : jsonObject.keySet()) {
                    Text mapKey = new Text(key.toString());
                    Text mapValue = new Text();
                    if (jsonObject.get(key) != null) {
                        mapValue.set(jsonObject.get(key).toString());
                    }
                    value.put(mapKey, mapValue);
                }
                return true;
            } catch (ParseException e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
