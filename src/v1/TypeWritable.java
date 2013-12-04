package v1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TypeWritable implements Writable {
	private double rank = 1;
	private int project = -1;
	public double getRank() {
		return rank;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}

	private TextArrayWritable imports = new TextArrayWritable();
	
	public TypeWritable(){
		setImports(new Text[0]);
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		rank = arg0.readDouble();
		project = arg0.readInt();
		imports.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeDouble(rank);
		arg0.writeInt(project);
		imports.write(arg0);
	}
	
	public Text[] getImports(){
		return (Text[]) imports.toArray();
	}
	
	public void setImports(Text[] imports){
		this.imports.set(imports);
	}
	
	public String toString(){
		return rank+": "+Arrays.toString(imports.toStrings());
	}

	public int getProject() {
		return project;
	}

	public void setProject(int project) {
		this.project = project;
	}

}

class TextArrayWritable extends ArrayWritable{

	public TextArrayWritable() {
		super(Text.class);
	}
	
	
}