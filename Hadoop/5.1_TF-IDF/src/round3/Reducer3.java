package round3;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.io.IntWritable;
import TFIDFDriver.ConstantesDef;
import utils.CompositeThreeElements;
import utils.CompositeTwoELements;
import utils.compositekeyforTFIDF;


public class Reducer3 extends Reducer<Text, CompositeThreeElements, CompositeTwoELements, Text> {

    @Override
    public void reduce(final Text key,Iterable<CompositeThreeElements> values,
            final Context context) throws IOException, InterruptedException {

        int sum = 0;
        Iterator<CompositeThreeElements> iterator = values.iterator();
        ArrayList<CompositeThreeElements> cache = new ArrayList<CompositeThreeElements>();
        
        while (iterator.hasNext()) {
        	CompositeThreeElements next = iterator.next();
        	sum += 1;
        	String docId = next.getFirst().toString();
            String wc = next.getSecond().toString();
            String docLength = next.getThird().toString();
            CompositeThreeElements saved = new CompositeThreeElements(docId,wc,docLength);
        	cache.add(saved);//add to cache for second loop

        }

        int size = cache.size();
        for (int i = 0; i<size; ++i) {
        	CompositeThreeElements it = cache.get(i);
            String docId = it.getFirst().toString();
            double wc = Integer.parseInt(it.getSecond());
            double docLength = Integer.parseInt(it.getThird());
            double nbr_doc_per_word = sum;
            double tfIDF = (wc/docLength)*Math.log10((ConstantesDef.NB_Documents/nbr_doc_per_word));
            CompositeTwoELements key_2 = new CompositeTwoELements(key.toString(),docId);
            String tfstr_str = Double.toString(tfIDF);
            Text value_2 = new Text();
            value_2.set(tfstr_str);
            context.write(key_2, value_2);
        }
        
    }
}
