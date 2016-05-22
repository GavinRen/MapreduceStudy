import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by 任贵福 on 2016/5/22.
 */
public class MyPartitioner extends Partitioner<Mykey,IntWritable> {
    public int getPartition(Mykey mykey, IntWritable integer, int i) {
        return Math.abs((mykey.getYear().hashCode()% i));
    }
}
