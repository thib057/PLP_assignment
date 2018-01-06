package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


/**
 * @author Thibaut DRAPP
 * inspired from SreeVeni U.B code
 *
 */
public class TwoTextWritable implements Writable {
    private String first;
    private String second;

    public  TwoTextWritable() {
        set(first, second);
    }
    public  TwoTextWritable(String first, String second) {
        set(first, second);
    }
    public void set(String first, String second) {
        this.first = first;
        this.second = second;
    }
    public String getFirst() {
        return first;
    }
    public String getSecond() {
        return second;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(first);
        out.writeBytes(second);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        String combinaison = in.readLine(); //combinaison of the two char
        first = combinaison.substring(0, 4);
        second = combinaison.substring(4);
    }
}