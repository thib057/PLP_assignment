package utils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class TroupleWrittable {
	public Text first = new Text();
	public Text second = new Text();
	public double TFIDF = 0;
	
	public TroupleWrittable(String first, String second, double TFIDF){
		this.first.set(first);
		this.second.set(second);
		this.TFIDF = TFIDF;
	}

}
