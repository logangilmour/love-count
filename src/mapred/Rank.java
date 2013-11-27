package mapred;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

public class Rank extends Configured implements Tool{
	private final float dangling;
	private final int count;
	public Rank(float dangling, int count){
		this.dangling=dangling;
		this.count=count;
	}

 public static class Map extends Mapper<Text, TypeWritable, Text, TypeWritable> {
	TypeWritable type = new TypeWritable();
	Text name = new Text();
    
    @Override
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
            value.setRank(0);
    	context.write(key, value);
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

 public static class Reduce extends Reducer<Text, TypeWritable, Text, TypeWritable> {
	 TypeWritable t = new TypeWritable();

    @Override
	public void reduce(Text key, Iterable<TypeWritable> values, Context context) 
    		throws IOException, InterruptedException {
		 float n = context.getConfiguration().getFloat("love.count", 0);
		 float danglingRank = context.getConfiguration().getFloat("love.dangling",0);
		 double pagerankParam = 0.85;
		 
		 double cur = 0;
    	t = new TypeWritable();
    	for(TypeWritable type: values){
    		
    		if(type.getImports().length>0){
    			this.t=type;
    		}
    		cur+=type.getRank();
    	}
    	//return (intermediate_key,
    	//          pr_param*sum(intermediate_value_list)+s*ip/n+(1.0-s)/n)
    	t.setRank(pagerankParam*cur+pagerankParam*danglingRank/n+(1.0-pagerankParam)/n);
    	context.write(key, t);
    }
 }
 
 public static class Combine extends Reducer<Text, TypeWritable, Text, TypeWritable> {
	 TypeWritable t = new TypeWritable();

    @Override
	public void reduce(Text key, Iterable<TypeWritable> values, Context context) 
    		throws IOException, InterruptedException {
		 
		 double cur = 0;
    	t = new TypeWritable();
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

@Override
public int run(String[] args) throws Exception {

	
	
    Job job = new Job();
    Configuration conf = job.getConfiguration();
    conf.setInt("love.count", this.count);
    conf.setFloat("love.dangling", this.dangling);
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
    job.setCombinerClass(Combine.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(Rank.class);
    job.waitForCompletion(true);
   
    return 0;
    }
}

