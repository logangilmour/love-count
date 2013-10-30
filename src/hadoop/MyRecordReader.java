package hadoop;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

public class MyRecordReader extends SequenceFileRecordReader<Text,Text> {
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean val = false;
		try{
			val = super.nextKeyValue();
		}catch(EOFException e){
			
		}
		return val;
	}
}
