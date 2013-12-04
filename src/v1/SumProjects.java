package v1;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

public class SumProjects extends Configured implements Tool{


 public static class Map extends Mapper<Text, TypeWritable, IntWritable, DoubleWritable> {
	DoubleWritable r = new DoubleWritable();
	IntWritable i = new IntWritable();
    
    @Override
	public void map(Text key, TypeWritable value, Context context) throws IOException, InterruptedException {
    	r.set(value.getRank());
    	i.set(value.getProject());
    	context.write(i, r);
    }

  @Override
public void run (Context context) throws IOException, InterruptedException {
        setup(context);
        while (context.nextKeyValue()) {
              map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
        cleanup(context);
  }
 }

 public static class Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    @Override
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) 
    		throws IOException, InterruptedException {
    	double rank=0;
    	for(DoubleWritable d: values){
    		rank+=d.get();
    	}
    	context.write(key, new DoubleWritable(rank));
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

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    conf.set("mapreduce.output.textoutputformat.separator", ",");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(SumProjects.class);
    job.waitForCompletion(true);
    
    return 0;
    }
}

