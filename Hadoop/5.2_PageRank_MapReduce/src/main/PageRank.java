package main;
import graph.adjacencymatrix.ConstructMatrixMapper;
import graph.adjacencymatrix.ConstructMatrixReducer;
import mapreduce.sorting.pattern.topkdesign.SortingMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import pagerank.mapreduce.PageRankMapper;
import pagerank.mapreduce.PageRankReducer;
import utils.Node;


public class PageRank extends Configured implements Tool {
	
	public final static double PAGE_RANK_INIT_VAL = (1/75879);
	public final static int NB_ITERATION = 20;
	public final static double NUMBER_OF_NODES = 75879;
	public final static double DAMPING = 0.85;
	public final static int TOP_N_RECORDS = 10; //number of records to keep in output

    public int run(String[] args) throws Exception {

        if (args.length != 2) {

            System.out.println("Usage: [input] [output]");

            System.exit(-1);
        }
        // Premier job : premier wordcount avec clé composite mot document

        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank");
        // On précise les classes MyProgram, Map et Reduce
        job.setJarByClass(PageRank.class);
        //job.setMapperClass(WordCountMapper.class);
        job.setMapperClass(ConstructMatrixMapper.class);
        //job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(ConstructMatrixReducer.class);
        // Définition des types clé/valeur de notre problème
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path("/user/thib057/temp/0");
     // On accepte une entree recursive
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);
        FileSystem fs = FileSystem.newInstance(new Configuration());
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        int jobEnded = job.waitForCompletion(true) ? 0: 1;

	    // Second job
        Path outputFilePath2 = null; //init
        Path inputFilePath2 = outputFilePath; //for first job
        for (int iter = 1; iter <= PageRank.NB_ITERATION; iter++) {
        	Job job2 = Job.getInstance(getConf());
    	    job2.setJobName("pagerank_mapreduce");
    	    // On précise les classes MyProgram, Map et Reduce
    	    job2.setJarByClass(PageRank.class);
    	    //job.setMapperClass(.class);
    	    job2.setMapperClass(PageRankMapper.class);
    	    job2.setReducerClass(PageRankReducer.class);
    	    // Définition des types clé/valeur de notre problème
    	    job2.setMapOutputKeyClass(IntWritable.class);
    	    job2.setMapOutputValueClass(Text.class);
    	    job2.setOutputKeyClass(IntWritable.class);
    	    job2.setOutputValueClass(Text.class);
    	    outputFilePath2 = new Path("/user/thib057/temp/" + String.valueOf(iter));
    	 // On accepte une entree recursive
    	    FileInputFormat.setInputDirRecursive(job2, true);
    	    FileInputFormat.addInputPath(job2, inputFilePath2);
    	    FileOutputFormat.setOutputPath(job2, outputFilePath2);
    	    if (fs.exists(outputFilePath2)) {
    	        fs.delete(outputFilePath2, true);
    	    }
    	    inputFilePath2 = outputFilePath2; //to chain jobs

    	
    	    int job2Result = job2.waitForCompletion(true) ? 0: 1;
        }
	    	    
	    
	    //SORTING MAPPER
	    Job job3 = Job.getInstance(getConf());
	    job3.setJobName("pagerank_mapreduce_sort");
	    // On précise les classes MyProgram, Map et Reduce
	    job3.setJarByClass(PageRank.class);
	    job3.setMapperClass(SortingMapper.class);
	    // Définition des types clé/valeur de notre problème
	    job3.setMapOutputKeyClass(DoubleWritable.class);
	    job3.setMapOutputValueClass(Text.class);
	    job3.setOutputKeyClass(DoubleWritable.class);
	    job3.setOutputValueClass(Text.class);
	    Path inputFilePath3 = outputFilePath2;
	    Path outputFilePath3 = new Path(args[1]);
	 // On accepte une entree recursive
	    FileInputFormat.setInputDirRecursive(job3, true);
	    FileInputFormat.addInputPath(job3, inputFilePath3);
	    FileOutputFormat.setOutputPath(job3, outputFilePath3);
	    if (fs.exists(outputFilePath3)) {
	        fs.delete(outputFilePath3, true);
	    }
	
	    return job3.waitForCompletion(true) ? 0: 1;
	    
}
    public static void main(String[] args) throws Exception {

        PageRank tfidf = new PageRank();

        int res = ToolRunner.run(tfidf, args);

        System.exit(res);

    }

}
