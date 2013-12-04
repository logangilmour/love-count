package v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ProjectWritable implements Writable {
	private double rank = 1;
	public double getRank() {
		return rank;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}

	private RefCountArray imports = new RefCountArray();
	
	public ProjectWritable(){
		setImports(new RefCount[0]);
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		rank = arg0.readDouble();
		imports.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeDouble(rank);
		imports.write(arg0);
	}
	
	public RefCount[] getImports(){
		return (RefCount[]) imports.toArray();
	}
	
	public void setImports(RefCount[] imports){
		this.imports.set(imports);
	}

}

class RefCount implements Writable{
	private int count=1;
	private int ref=-1;
	@Override
	public void readFields(DataInput arg0) throws IOException {
		count = arg0.readInt();
		ref = arg0.readInt();
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(count);
		arg0.writeInt(ref);
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getRef() {
		return ref;
	}

	public void setRef(int ref) {
		this.ref = ref;
	}
	
	public String toString(){
		return ref+","+count;
	}
	
}

class RefCountArray extends ArrayWritable{
	public RefCountArray(){
		super(RefCount.class);
	}
}