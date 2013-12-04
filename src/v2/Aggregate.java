package v2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

import v1.Rank.Combine;

public class Aggregate extends Configured implements Tool{

 public static class Map extends Mapper<Text, CitationAggregator, IntWritable, ProjectWritable> {
    IntWritable c = new IntWritable();
    ProjectWritable o = new ProjectWritable();
	RefCount imp[] = new RefCount[1];
	RefCount empty[] = new RefCount[0];

    @Override
	public void map(Text key, CitationAggregator value, Context context) throws IOException, InterruptedException {
    	c.set(value.getCiter());
    	if(value.getOwner()!=-1){
    		RefCount r = new RefCount();
    		r.setRef(value.getOwner());
    		imp[0]=r;
    		o.setImports(imp);
    	}else{
    		o.setImports(empty);
    	}
    	context.write(c,o);
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

 public static class Reduce extends Reducer<IntWritable, ProjectWritable, IntWritable, ProjectWritable> {
	 ProjectWritable t = new ProjectWritable();

    @Override
	public void reduce(IntWritable key, Iterable<ProjectWritable> values, Context context) 
    		throws IOException, InterruptedException {
		 
		
    	t = new ProjectWritable();
    	HashMap<Integer,Integer> deps = new HashMap<Integer,Integer>();
    	
    	for(ProjectWritable proj: values){
    		for(RefCount r: proj.getImports()){
    			Integer e = deps.get(r.getRef());
    			if(e!=null) deps.put(r.getRef(), r.getCount()+e);
    			else deps.put(r.getRef(), r.getCount());
    		}
    	}
    	ArrayList<RefCount> refs = new ArrayList<RefCount>();
    	for(Entry<Integer,Integer> e:deps.entrySet()){
    		RefCount r = new RefCount();
    		r.setRef(e.getKey());
    		r.setCount(e.getValue());
    		refs.add(r);
    	}
    	t.setImports(refs.toArray(new RefCount[0]));
    	
    	//return (intermediate_key,
    	//          pr_param*sum(intermediate_value_list)+s*ip/n+(1.0-s)/n)
    	context.write(key, t);
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
    job.setOutputValueClass(ProjectWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(Aggregate.class);
    job.waitForCompletion(true);
   
    return 0;
    }

 
}

