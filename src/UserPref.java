/**
 * Created by qiuhaoling on 11/28/15.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class UserPref {
    public static void main(String[] args) throws Exception {
        Job job = new Job(new Configuration(),"UserPref");
        job.setJobName("UserPref");
        job.setJarByClass(UserPref.class);

        FileInputFormat.setInputDirRecursive(job,true);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,lastfm.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,tasteprofile.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(GenreJoin.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);
        Log log = LogFactory.getLog(UserPref.class);
        log.info("Hello World!");
    }
}