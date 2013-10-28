package a2;
import japa.parser.JavaParser;
import japa.parser.ParseException;
import japa.parser.ast.CompilationUnit;
import japa.parser.ast.ImportDeclaration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.eclipse.jdt.internal.compiler.parser.JavadocParser;

import com.sun.el.parser.TokenMgrError;

public class FileCount extends Configured implements Tool{

 public static class Map extends Mapper<Text, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

    		       	    	
    	Text im = new Text();
    	Text bad = new Text("Bad Count");
    	CompilationUnit unit =null;
    	ByteArrayInputStream b = null;
		
    	
    	try {
    		b = new ByteArrayInputStream(value.toString().getBytes("UTF-8"));
			unit = JavaParser.parse(b);
	    	for(ImportDeclaration dec: unit.getImports()){	    		
	    		im.set(dec.toString());
	    		context.write(im, one);
	    	}

		} catch (ParseException e) {
			context.write(bad, one);
		}catch (NullPointerException e){
			context.write(bad, one);
		}catch (Error e){
			context.write(bad, one);
		}
		
    	

    }

  public void run (Context context) throws IOException, InterruptedException {
        setup(context);
        while (context.nextKeyValue()) {
              map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
        cleanup(context);
  }
 }

 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }

public int run(String[] args) throws Exception {

	
	
    Job job = new Job();

    FileSystem hdfs = FileSystem.get(job.getConfiguration());
	FileStatus[] jarFiles = hdfs.listStatus(new Path("/libs"));
	 for (FileStatus fs : jarFiles) {
	      Path disqualified = new Path(fs.getPath().toUri().getPath());
	      DistributedCache.addFileToClassPath(disqualified, job.getConfiguration(), hdfs);
	 }

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);

    job.setInputFormatClass(TolerantSequenceFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(FileCount.class);

    job.submit();
    return 0;
    }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    ToolRunner.run(new FileCount(), otherArgs);
 }
}

