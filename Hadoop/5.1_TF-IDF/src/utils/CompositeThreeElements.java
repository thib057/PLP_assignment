package utils;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CompositeThreeElements implements WritableComparable<CompositeThreeElements> {

        public Text first = new Text(),
               second = new Text(),
               third = new Text();

        public CompositeThreeElements(){
        	
        }
        
        public CompositeThreeElements(String first, String second, String third) {
            this.first.set(first);
            this.second.set(second);
            this.third.set(third);
        }

        public String getFirst() {
            return this.first.toString();
        }

        public String getSecond() {
            return this.second.toString();
        }
        
        public String getThird() {
            return this.third.toString();
        }


        public void write(DataOutput out) throws IOException {
            first.write(out);
            second.write(out);
            third.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            first.readFields(in);
            second.readFields(in);
            third.readFields(in);
        }

        public int compareTo(CompositeThreeElements other) {

      /* Compare keys first by term,
         then by docID.
      */

            int ret = first.compareTo(other.first);
            if (ret == 0) {
                ret = second.compareTo(other.second);
            }
            if (ret == 0) {
            	ret = third.compareTo(other.third);
            }
            return ret;
        }

        public String toString() {
            return first + "\t" + second + "\t" + third;
        }

        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            CompositeThreeElements other = (CompositeThreeElements) obj;
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
            if (third == null) {
                if (other.third != null)
                    return false;
            } else if (!third.equals(other.third))
                return false;
            return true;
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((first == null) ? 0 : first.hashCode());
            result = prime * result + ((second == null) ? 0 : second.hashCode());
            result = prime * result + ((third == null) ? 0 : third.hashCode());
            return result;
        }
}
