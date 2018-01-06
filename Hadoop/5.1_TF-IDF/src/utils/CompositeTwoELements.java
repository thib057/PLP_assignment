package utils;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CompositeTwoELements implements WritableComparable<CompositeTwoELements> {

        public Text first = new Text(),
               second = new Text();

        public CompositeTwoELements(){
        	
        }
        
        public CompositeTwoELements(String first, String second) {
            this.first.set(first);
            this.second.set(second);
        }

        public String getFirst() {
            return this.first.toString();
        }

        public String getSecond() {
            return this.second.toString();
        }


        public void write(DataOutput out) throws IOException {
            first.write(out);
            second.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            first.readFields(in);
            second.readFields(in);
        }

        public int compareTo(CompositeTwoELements other) {

      /* Compare keys first by term,
         then by docID.
      */

            int ret = first.compareTo(other.first);
            if (ret == 0) {
                ret = second.compareTo(other.second);
            }
            return ret;
        }

        public String toString() {
            return first + "\t" + second;
        }

        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            CompositeTwoELements other = (CompositeTwoELements) obj;
            if (first == null) {
                if (other.first != null)
                    return false;
            } else if (!first.equals(other.first))
                return false;
            if (second == null) {
                if (other.second != null)
                    return false;
            } else if (!second.equals(other.second))
                return false;
            return true;
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((first == null) ? 0 : first.hashCode());
            result = prime * result + ((second == null) ? 0 : second.hashCode());
            return result;
        }
}
