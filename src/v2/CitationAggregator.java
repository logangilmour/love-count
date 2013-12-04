package v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CitationAggregator implements Writable {
	private int citer = -1;
	private int owner = -1;
	
	public CitationAggregator(){
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		
		owner=arg0.readInt();
		citer=arg0.readInt();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(owner);
		arg0.writeInt(citer);
		
	}

	public int getCiter() {
		return citer;
	}

	public void setCiter(int citer) {
		this.citer = citer;
	}

	public int getOwner() {
		return owner;
	}

	public void setOwner(int owner) {
		this.owner = owner;
	}
	
	

}

