package TFIDFDriver;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import round2.Mapper2;
import round2.Reducer2;
import round3.Mapper3;
import round3.Reducer3;
import round2.Reducer2bis;
import DisplayingSorting.MapperSorter;
import DisplayingSorting.ReducerSorter;
import utils.CompositeThreeElements;
import utils.CompositeTwoELements;
import utils.compositekeyforTFIDF;
import wordcount.WordCountMapper;
import wordcount.WordCountReducer;

public class TFIDF extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 5) {
        	// output1 : mot+docID+wordcount
        	// output2 : mot+docID+wordcount+tailledoc
        	// output3 : mot+docID+TFIDF
        	// output4 : 20 maxima mot+docID+TFIDF par rapport au TFIDF
            System.out.println("Usage: [input] [output1] [output2] [output3] [output4]");

            System.exit(-1);
        }
        // 		FIRST JOB

        // Wordcount avec clé composite. Mapper -> key:<mot-docID> Value:<1>

        Job job = Job.getInstance(getConf());
        job.setJobName("WordCount");
        // On précise les classes MyProgram, Map et Reduce
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // Définition des types clé/valeur de notre problème
        job.setMapOutputKeyClass(compositekeyforTFIDF.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(compositekeyforTFIDF.class);
        job.setOutputValueClass(IntWritable.class);
        
        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);
        
        //Initialisation du nombre de documents dans le corpus
        File directory = new File(inputFilePath.toString());
        ConstantesDef.NB_Documents = directory.list().length;
        System.out.println("Inputs-File Count :"+ConstantesDef.NB_Documents);
        //On accepte une entree recursive
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);
        FileSystem fs = FileSystem.newInstance(new Configuration());
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        int wait = job.waitForCompletion(true) ? 0: 1;

        // 		SECOND JOB
        
	    //Determination du nombre de mots par document. Mapper -> key:<docID> Value:<mot, wordcount>
	
	    Job job2 = Job.getInstance(getConf());
	    job2.setJobName("round2");
	    // On précise les classes MyProgram, Map et Reduce
	    job2.setJarByClass(TFIDF.class);
	    job2.setMapperClass(Mapper2.class);
	    job2.setReducerClass(Reducer2bis.class);
	    // Définition des types clé/valeur de notre problème
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(CompositeTwoELements.class);
	    job2.setOutputKeyClass(CompositeTwoELements.class);
	    job2.setOutputValueClass(CompositeTwoELements.class);

	    Path inputFilePath2 = outputFilePath;
	    Path outputFilePath2 = new Path(args[2]);
	    // On accepte une entree recursive
	    FileInputFormat.setInputDirRecursive(job2, true);
	    FileInputFormat.addInputPath(job2, inputFilePath2);
	    FileOutputFormat.setOutputPath(job2, outputFilePath2);
	    if (fs.exists(outputFilePath2)) {
	        fs.delete(outputFilePath2, true);
	    }
	    int wait2 = job2.waitForCompletion(true) ? 0: 1;
    
	    // 		THIRD JOB
	    
	    // Détermination du nombre de documents par mot. Mapper -> key:<mot> Value:<docID, wordcount, tailleDoc>
		//												 Reducer -> key:<mot, docID> Value:<TFIDF>
	    Job job3 = Job.getInstance(getConf());
	    job3.setJobName("round3");
	    // On précise les classes MyProgram, Map et Reduce
	    job3.setJarByClass(TFIDF.class);
	    job3.setMapperClass(Mapper3.class);
	    job3.setReducerClass(Reducer3.class);
	    // Définition des types clé/valeur de notre problème
	    job3.setMapOutputKeyClass(Text.class);
	    job3.setMapOutputValueClass(CompositeThreeElements.class);
	    job3.setOutputKeyClass(CompositeTwoELements.class);
	    job3.setOutputValueClass(Text.class);
	    
	    Path inputFilePath3 = outputFilePath2;
	    Path outputFilePath3 = new Path(args[3]);
	    // On accepte une entree recursive
	    FileInputFormat.setInputDirRecursive(job3, true);
	    FileInputFormat.addInputPath(job3, inputFilePath3);
	    FileOutputFormat.setOutputPath(job3, outputFilePath3);
	    if (fs.exists(outputFilePath3)) {
	        fs.delete(outputFilePath3, true);
	    }
		int wait3 = job3.waitForCompletion(true) ? 0: 1;
	
		//		DISPLAY JOB
		
		// On retient les 20 premiers couples <mot, docId> en terme de tf-idf
		
	    Job job4 = Job.getInstance(getConf());
	    job4.setJobName("Display");
	    // On précise les classes MyProgram, Map et Reduce
	    job4.setJarByClass(TFIDF.class);
	    job4.setMapperClass(MapperSorter.class);
	    job4.setReducerClass(ReducerSorter.class);
	    // Définition des types clé/valeur de notre problème
	    job4.setMapOutputKeyClass(IntWritable.class);
	    job4.setMapOutputValueClass(CompositeThreeElements.class);
	    job4.setOutputKeyClass(CompositeTwoELements.class);
	    job4.setOutputValueClass(Text.class);
	    
	    Path inputFilePath4 = outputFilePath3;
	    Path outputFilePath4 = new Path(args[4]);
	    // On accepte une entree recursive
	    FileInputFormat.setInputDirRecursive(job4, true);
	    FileInputFormat.addInputPath(job4, inputFilePath4);
	    FileOutputFormat.setOutputPath(job4, outputFilePath4);
	    if (fs.exists(outputFilePath4)) {
	        fs.delete(outputFilePath4, true);
	    }
    return job4.waitForCompletion(true) ? 0: 1;
    }
    
    public static void main(String[] args) throws Exception {

        TFIDF tfidf = new TFIDF();

        int res = ToolRunner.run(tfidf, args);

        System.exit(res);

    }

}
