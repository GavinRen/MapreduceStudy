import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by 任贵福 on 2016/5/22.
 */
public class MyGroupingComparator extends WritableComparator {
    public MyGroupingComparator(){
        super(Mykey.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Mykey mykey1 =(Mykey) a;
        Mykey mykey2 =(Mykey) b;
        int result = mykey1.getYear().compareTo(mykey2.getYear());
        if (result==0){
            result = mykey1.compare(mykey1.getMonth(),mykey2.getMonth());
        }
        return  result;

    }
}
