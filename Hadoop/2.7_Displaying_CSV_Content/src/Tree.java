import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

//we define Tree as abstract (we would not be able to instantiate it), as demanded in the subject
public abstract class Tree{
	public static String year_tree; //parameters are declared in static in order to access them without instance of the class
	public static String height_tree;
	
	public static void set_tree(String year, String height) {
		//set height and year of the tree. If empty, set to unknown
		if (!year.isEmpty()){
				year_tree = year;
		}else {
			year_tree = "Unknown";
		}
		if (!height.isEmpty()) {
			height_tree= height;
		}else {
			height_tree = "Unknown";
		}
	}
	
	public static void display_tree() {
		// print the tree values
		System.out.println("Year : "+year_tree+ " \t Height : " + height_tree);
	}
	
	public static String getLine() {
		//return the line
		return "Year : "+year_tree+ " \t Height : " + height_tree;
	}
	
	public static void main(String[] args) throws IOException {
		
		//STANDALONE MODE
		String localSrc = "./data/arbres.csv"; //relative path
		FileOutputStream out = new FileOutputStream("result/output.txt"); //to write the result
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			br.readLine(); //skip first line (hauteur, annee) useless
			// read line by line
			String line = br.readLine();
			
			while (line != null){
				// Process of the current line
				String year = line.split(";")[5];
				String height = line.split(";")[6];
				set_tree(year, height);
				display_tree();
				// write the result and go to the next line
				out.write((getLine() + "\n").getBytes());
				line = br.readLine();

			}	
		}
		finally{
			//close the files
			in.close();
			out.close();
			fs.close();
		}	
	}
}