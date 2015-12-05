import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by qiuhaoling on 11/29/15.
 */
public class GenreJoin extends Reducer{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //Text outputKey = new Text();
        ArrayList<String> users = new ArrayList<String>();
        Text outputValue = new Text();
        String tmp;
        if(!values.iterator().hasNext())return;
        while(values.iterator().hasNext())
        {
            tmp = values.iterator().next().toString();
            if(tmp.length() == 40)
            {
                //outputKey.set(tmp);
                users.add(tmp);
                continue;
            }
            else
            {
                outputValue.set(tmp);
            }
        }
        for(String it : users)
        {
            if(!outputValue.toString().isEmpty())
                context.write(new Text(it),outputValue);
        }
    }
}
