import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 任贵福 on 2016/5/21.
 */
public class Mykey implements WritableComparable<Mykey> {
    private String year;
    private int month;
    private int days;
    public Mykey(){}
    public int compareTo(Mykey o) {
        int result =this.year.compareTo(o.year);
        if (result==0){
            result=compare(this.month,o.month);
        }
        if (result==0){
            result=compare(this.days,o.month);
        }
        return result;
    }
    public int compare(int a, int b){
        return a >b ? 1:(a < b ? -1 :0);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(days);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.year=dataInput.readUTF();
        this.month = dataInput.readInt();
        this.days =dataInput.readInt();
    }

    public int getMonth() {
        return month;
    }

    public String getYear() {
        return year;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public int getDays() {
        return days;
    }

    public void setDays(int days) {
        this.days = days;
    }

    @Override
    public String toString() {
        return year+"\t"+month;
    }
}
