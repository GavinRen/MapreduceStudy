import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 任贵福 on 2016/5/22.
 */
public class MyReducer extends Reducer<Mykey,IntWritable,Mykey,IntWritable> {
    @Override
    protected void reduce(Mykey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value:values){
            context.write(key,value);
        }
    }
}
