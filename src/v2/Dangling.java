package v2;
import java.io.IOException;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

public class Dangling extends Configured implements Tool{
static Text output = new Text("output");
 public static class Map extends Mapper<IntWritable, ProjectWritable, Text, DoubleWritable> {

    @Override
    public void run (Context context) throws IOException, InterruptedException {
    	  
            setup(context);
            double rank = 0;
            while (context.nextKeyValue()) {
                  ProjectWritable w = context.getCurrentValue();
                  if(w.getImports().length<1){
                	  rank+=w.getRank();
                  }
                }
            context.write(new Text("count"), new DoubleWritable(rank));
            cleanup(context);
      }
 }

 public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
    		throws IOException, InterruptedException {
		 
		 double cur = 0;
    	for(DoubleWritable val: values){
    		cur+=val.get();
    	}

    	context.write(output, new DoubleWritable(cur));
    }
 }

@Override
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
    job.setOutputValueClass(DoubleWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    //job.setCombinerClass(Reduce.class);
    
    //job.setNumReduceTasks(1);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(Dangling.class);
    job.waitForCompletion(true);
   
    return 0;
    }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    ToolRunner.run(new Dangling(), otherArgs);
 }
}

