package utils;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class compositekeyforTFIDF implements WritableComparable<compositekeyforTFIDF> {

        public Text word = new Text(),
               docId = new Text();

        public compositekeyforTFIDF(){
        	
        }
        
        public compositekeyforTFIDF(String term, String docID) {
            this.word.set(term);
            this.docId.set(docID);
        }

        public String getTerm() {
            return this.word.toString();
        }

        public String getDocID() {
            return this.docId.toString();
        }


        public void write(DataOutput out) throws IOException {
            word.write(out);
            docId.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            word.readFields(in);
            docId.readFields(in);
        }

        public int compareTo(compositekeyforTFIDF other) {

      /* Compare keys first by term,
         then by docID.
      */

            int ret = word.compareTo(other.word);
            if (ret == 0) {
                ret = docId.compareTo(other.docId);
            }
            return ret;
        }

        public String toString() {
            return word + "\t" + docId;
        }

        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            compositekeyforTFIDF other = (compositekeyforTFIDF) obj;
            if (word == null) {
                if (other.word != null)
                    return false;
            } else if (!word.equals(other.word))
                return false;
            if (docId == null) {
                if (other.docId != null)
                    return false;
            } else if (!docId.equals(other.docId))
                return false;
            return true;
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((word == null) ? 0 : word.hashCode());
            result = prime * result + ((docId == null) ? 0 : docId.hashCode());
            return result;
        }
}
