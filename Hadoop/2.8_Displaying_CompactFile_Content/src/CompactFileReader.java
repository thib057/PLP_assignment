


import java.io.*;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public abstract class CompactFileReader {
	
	private static String usafCode;
	private static String name;
	private static String country;
	private static String elevation;

	public static String getResult(){
		String result =  usafCode + "\t"+ name +"\t"+ country +"\t"+ elevation;
		return result;
	}
	
	public static void main(String[] args) throws IOException {
		
		//Get the conf, create Path for HDFS
		Configuration conf = new Configuration();
		Path hdfsSrc = new Path(args[0]); //Path created for finding file on HDFS
		Path dst = new Path(args[1]); //destination path to save the result
		FileSystem fs = FileSystem.get(conf);

		//Create input stream from HDFS file
		FSDataInputStream in = fs.open(hdfsSrc);
		FSDataOutputStream out = fs.create(dst);
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
		
			//for loop to skip the first 22 lines which contain no data
			for (int counter = 0; counter <22; counter++){
					br.readLine();
			}
			
			//write headers to final file
			out.writeBytes("usafCode \t name \t \t    country \t elevation \n");
			
			String line = br.readLine();
			while (line !=null){
				//get the informations
				usafCode = line.substring(0,6);
				name = line.substring(13, 42);
				country = line.substring(43,45);
				elevation = line.substring(74, 81);
				// Print the line
				System.out.println(getResult());
				
				// Write the output
				out.writeBytes(getResult()+"\n");
				out.flush();
				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
			out.close();
		}
 
		
		
	}

}
