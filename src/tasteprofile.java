import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.tools.ant.types.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by qiuhaoling on 11/29/15.
 */
public class tasteprofile extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    private HashMap<String,Pair<String,Integer> > recoder;
    public void setup(Context context) throws IOException, InterruptedException {
        recoder = new HashMap<String,Pair<String,Integer> >();
    }
    public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
        String inputStr[] = value.toString().split("\t");
        Pair<String,Integer> entry = null;
        entry = recoder.get(inputStr[0]);
        if(entry == null)
        {
            recoder.put(inputStr[0],new Pair<String, Integer>(inputStr[1],Integer.parseInt(inputStr[2])));
            return;
        }
        if(entry.getSecond()<Integer.parseInt(inputStr[2]))
        {
            recoder.put(inputStr[0],new Pair<String, Integer>(inputStr[1],Integer.parseInt(inputStr[2])));
            return;
        }
        return;
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<String,Pair<String,Integer>> en: recoder.entrySet())
        {
            context.write(new Text(en.getValue().getFirst()),new Text(en.getKey()));
        }
    }
}
