import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by 任贵福 on 2016/5/22.
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        Path input = new Path("hdfs://bigdata-server:8020/test/groupsort/input/");
        Path output = new Path("hdfs://bigdata-server:8020/test/groupsort/output");
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) {
            fs.delete(output);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setJobName("GroupSort");

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //job.setCombinerClass(ConcersListReducer.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setOutputKeyClass(Mykey.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Mykey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
