/**
 * Created by qiuhaoling on 11/28/15.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class lastfm extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse
        LastFM_Format entry = mapper.readValue(value.toString(), LastFM_Format.class);
        if (!entry.tags.isEmpty()) {
            Text outputKey = new Text();
            int outputValue = 0;
            for (ArrayList<String> entry2 : entry.tags) {
                //context.write(new Text(entry2.get(0)), new Text(entry2.get(1)));
                if (Integer.parseInt(entry2.get(1)) > outputValue) {
                    outputValue = Integer.parseInt(entry2.get(1));
                    outputKey.set(entry2.get(0));
                }
            }
            context.write(new Text(entry.track_id),outputKey);
        }
    }

    public static class LastFM_Format {
        public String artist;
        public String timestamp;
        public ArrayList<ArrayList<String>> similars;
        public ArrayList<ArrayList<String>> tags;
        public String track_id;
        public String title;
    }

}
