import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by 任贵福 on 2016/5/22.
 */
public class MyMapper extends Mapper<LongWritable,Text,Mykey,IntWritable> {
    private Mykey mykey=new Mykey();
    private IntWritable valueout= new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line =value.toString().trim();
        String[] tokens = line.split(",");
        if (tokens.length==3){
            String year =tokens[0];
            int month =Integer.parseInt(tokens[1]);
            int days =Integer.parseInt(tokens[2]);
            mykey.setYear(year);
            mykey.setMonth(month);
            mykey.setDays(days);
            valueout.set(days);
            context.write(mykey,valueout);

        }
    }
}
