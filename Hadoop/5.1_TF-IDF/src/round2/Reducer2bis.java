package round2;

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

import utils.CompositeThreeElements;
import utils.CompositeTwoELements;
import utils.compositekeyforTFIDF;


public class Reducer2bis extends Reducer<Text, CompositeTwoELements, CompositeTwoELements, CompositeTwoELements> {

    @Override
    public void reduce(final Text key,Iterable<CompositeTwoELements> values,
            final Context context) throws IOException, InterruptedException {

        int sum = 0;
        Iterator<CompositeTwoELements> iterator = values.iterator();
        ArrayList<CompositeTwoELements> cache = new ArrayList<CompositeTwoELements>();
        
        while (iterator.hasNext()) {
        	CompositeTwoELements next = iterator.next();
        	sum += Integer.parseInt(next.getSecond());
        	
        	String word = next.getFirst().toString();
            String wc = next.getSecond().toString();
        	CompositeTwoELements saved = new CompositeTwoELements(word,wc);
        	cache.add(saved);//add to cache for second loop

        }
        
        String sum_str = Integer.toString(sum);

        int size = cache.size();
        for (int i = 0; i<size; ++i) {
        	CompositeTwoELements it = cache.get(i);
            String word = it.getFirst().toString();
            String wc = it.getSecond().toString();
            CompositeTwoELements key_2 = new CompositeTwoELements(word, key.toString());
            CompositeTwoELements value_2 = new CompositeTwoELements(wc, sum_str);
            context.write(key_2, value_2);
        }
        
    }
}
