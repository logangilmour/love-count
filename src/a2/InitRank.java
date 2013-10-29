package a2;
import japa.parser.JavaParser;
import japa.parser.ParseException;
import japa.parser.ast.CompilationUnit;
import japa.parser.ast.ImportDeclaration;
import japa.parser.ast.PackageDeclaration;
import japa.parser.ast.body.TypeDeclaration;

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

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.eclipse.jdt.internal.compiler.parser.JavadocParser;

import a2.FileCount.Map.MyCounter;

import com.sun.el.parser.TokenMgrError;

public class InitRank extends Configured implements Tool{

 public static class Map extends Mapper<Text, Text, Text, TypeWritable> {
	TypeWritable type = new TypeWritable();
    public static enum MyCounter{
    	BAD_PARSE, NO_PACKAGE, WILD_CARD_IMPORTS, NO_IMPORTS
    };
    
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

    	TypeWritable type = new TypeWritable();
    	Text name = new Text();
    	
    	CompilationUnit unit =null;
    	ByteArrayInputStream b = null;
		
    	
    	try {
    		b = new ByteArrayInputStream(value.toString().getBytes("UTF-8"));
			unit = JavaParser.parse(b);
			PackageDeclaration dec = unit.getPackage();
			List<TypeDeclaration> types = unit.getTypes();
			List<ImportDeclaration> imports = unit.getImports();

			if(types!=null&&types.size()>0){
				name.set(dec.getName().toString()+"."+types.get(0).getName());
				ArrayList<Text> list = new ArrayList<Text>();

				if(imports != null){					
					for(ImportDeclaration declaration: imports){
						String decName = declaration.getName().toString();
						if(decName.contains("*")){
							context.getCounter(MyCounter.WILD_CARD_IMPORTS).increment(1);
						}else{
							list.add(new Text(decName));
						}
					}
					
		    	}else{
		    		context.getCounter(MyCounter.NO_IMPORTS).increment(1);
		    	}
				type.setImports(list.toArray(new Text[0]));
	    		context.write(name, type);
			}else{
				context.getCounter(MyCounter.NO_PACKAGE).increment(1);
			}
		} catch (ParseException e) {
			
			context.getCounter(MyCounter.BAD_PARSE).increment(1);
		}catch (NullPointerException e){
			context.getCounter(MyCounter.NO_PACKAGE).increment(1);
		}catch (Error e){
			context.getCounter(MyCounter.BAD_PARSE).increment(1);

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

 public static class Reduce extends Reducer<Text, TypeWritable, Text, TypeWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {

    }
 }

public int run(String[] args) throws Exception {

	
	
    Job job = new Job();
    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
	FileStatus[] jarFiles = fs.listStatus(new Path("/libs"));
	 for (FileStatus status : jarFiles) {
	      Path disqualified = new Path(status.getPath().toUri().getPath());
	      DistributedCache.addFileToClassPath(disqualified, conf, fs);
	 }

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TypeWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TolerantSequenceFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(InitRank.class);
    
    job.waitForCompletion(true);
    return 0;
    }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    ToolRunner.run(new InitRank(), otherArgs);
 }
}

