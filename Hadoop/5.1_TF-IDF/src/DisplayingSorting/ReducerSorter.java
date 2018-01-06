package DisplayingSorting;

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
import utils.TroupleWrittable;
import utils.compositekeyforTFIDF;


public class ReducerSorter extends Reducer<IntWritable, CompositeThreeElements, CompositeTwoELements, Text> {

    @Override
    public void reduce(final IntWritable key,Iterable<CompositeThreeElements> values,
            final Context context) throws IOException, InterruptedException {

        Iterator<CompositeThreeElements> iterator = values.iterator();
        ArrayList<TroupleWrittable> cache = new ArrayList<TroupleWrittable>();
        ArrayList<TroupleWrittable> maxima = new ArrayList<TroupleWrittable>();
        
        while (iterator.hasNext()) {
        	CompositeThreeElements next = iterator.next();
        	String word = next.getFirst().toString();
            String docId = next.getSecond().toString();
            String tfstr = next.getThird().toString();
            double tfidf = Double.parseDouble(tfstr);
            TroupleWrittable saved = new TroupleWrittable(word,docId,tfidf);
        	cache.add(saved);//add to cache for second loop
        }
        // On récupère les maxima
        for( int k=0; k<ConstantesDef.NB_WORDS_TO_DISPLAY;++k){
        	TroupleWrittable temp = new TroupleWrittable(cache.get(0).first.toString(),cache.get(0).second.toString(),cache.get(0).TFIDF);
            double temp_tfidf = temp.TFIDF;
        	int temp_idx = 0;
        	
	        int size = cache.size();
	        for (int i = 1; i<size; ++i) {
	        	TroupleWrittable it = cache.get(i);;
	            double tfidf = it.TFIDF;
	            
	            if( tfidf>=temp_tfidf){
	            	temp_idx = i;
	            	temp_tfidf=tfidf;
	            	temp.first = it.first;
	            	temp.second = it.second;
	            	temp.TFIDF = temp_tfidf;
	            }
	        }
	        cache.remove(temp_idx);
	        maxima.add(temp);
	        //System.out.println("cache size :" + size);
        }
        //On emet les maxima
        int nbr_max = maxima.size();
        System.out.println("Nombre de max :" + nbr_max);
        for( int i=0; i<nbr_max ; ++i){
        	TroupleWrittable val = maxima.get(i);
        	CompositeTwoELements key2 = new CompositeTwoELements(val.first.toString(),val.second.toString());
        	Text value2 = new Text(Double.toString(val.TFIDF));
        	context.write(key2, value2);
        }
    }
}