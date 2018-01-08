package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import main.PageRank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


/**
 * @author Thibaut DRAPP
 *
 *
 */
public class Node implements Writable {
    private ArrayList<Integer> adjList = new ArrayList<Integer>();
    private double pageRank = PageRank.PAGE_RANK_INIT_VAL; //needed

    //empty constructor for serialization
    public Node() {
    }
    public Node(double pageRank) {
    	this.pageRank = pageRank;
    }
    
    public void addNode(int nid) {
        this.adjList.add(nid);
    }
    
    public ArrayList<Integer> getAdjList(){
    	return this.adjList;
    }
    
	public void setPageRank(double nodePageRank) {
		this.pageRank = nodePageRank;
	}
	
	public double getPageRank() {
		return this.pageRank;
	}
	
    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeInt(this.adjList.size()); //need it to reconstruct the object in the readFields
    	for(int i=0; i<this.adjList.size(); i++){
            out.writeInt(this.adjList.get(i));
    	}
    	out.writeDouble(this.pageRank);
    	
    }
    @Override
    public void readFields(DataInput in) throws IOException {
    	int size = in.readInt();
    	this.pageRank = in.readDouble();
        for(int i=0; i<size; i++){
        	this.adjList.add(in.readInt());
        }
    }
    
    @Override
    public String toString() {
    	String result = "";
    	boolean first = true; //to construct string correctly
    	for (int i=0; i<this.adjList.size(); i++){
    		if(!first){
    			result += " ";
    		}
    		result += String.valueOf(this.adjList.get(i));
    		first = false;
    	}
    	return result;
    }


}