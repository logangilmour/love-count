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

import a2.FileSizes.Map.MyCounter;

import com.sun.el.parser.TokenMgrError;

public class Rank extends Configured implements Tool{

 public static class Map extends Mapper<Text, TypeWritable, Text, TypeWritable> {
	TypeWritable type = new TypeWritable();
	Text name = new Text();
    public static enum MyCounter{
    	BAD_PARSE, NO_PACKAGE, WILD_CARD_IMPORTS, NO_IMPORTS
    };
    
    public void map(Text key, TypeWritable value, Context context) throws IOException, InterruptedException {
    	Text[] imports = value.getImports();
    	
    	double rank = value.getRank();

            if (imports.length > 0) {
                    double share = rank / imports.length;
                    for (Text imp : imports) {
                            type.setRank(share);
                            context.write(imp, type);
                    }
            }
    	context.write(key, value);
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
	 TypeWritable t = new TypeWritable();
    public void reduce(Text key, Iterable<TypeWritable> values, Context context) 
    		throws IOException, InterruptedException {
    	double cur = 0;
    	
    	for(TypeWritable type: values){
    		if(type.getImports().length>0){
    			this.t=type;
    		}
    		cur+=type.getRank();
    	}
    	t.setRank(cur);
    	context.write(key, t);
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
    job.setCombinerClass(Reduce.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(Rank.class);
    job.waitForCompletion(true);
   
    return 0;
    }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    ToolRunner.run(new Rank(), otherArgs);
 }
}

