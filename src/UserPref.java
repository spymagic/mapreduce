/**
 * Created by qiuhaoling on 11/28/15.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
public class UserPref {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        //job.setJarByClass(UserPref.class);
        job.setJobName("UserPref");

        job.setJarByClass(UserPref.class);
        job.setMapperClass(lastfm.class);
        //job.setReducerClass(FlyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
        FileInputFormat.setInputDirRecursive(job,true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        Log log = LogFactory.getLog(UserPref.class);
        log.info("Hello World!");
    }
}