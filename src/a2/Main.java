package a2;

import mapred.Count;
import mapred.Dangling;
import mapred.InitRank;
import mapred.Rank;
import mapred.SumProjects;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

public class Main {
	

        public static void main(String[] args) throws Exception {
                
                StopWatch timer = new StopWatch();
                timer.start();
                Configuration conf = new Configuration();     
                FileSystem fs = FileSystem.get(conf);

                
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                
                String dir = otherArgs[1];
                
                // first, count all of the things to set the initial value


                
                // Then, set each rank up to be that initial value
                String[] initOpts = { otherArgs[0], dir + "tr-0.out" };
                ToolRunner.run(new InitRank(), initOpts);
                
                String [] countOpts = { dir + "tr-0.out", dir+ "cr.out"};
                ToolRunner.run(new Count(), countOpts);
                
                SequenceFile.Reader r = new SequenceFile.Reader(fs, new Path(dir+"cr.out/part-r-00000"), conf);
                IntWritable c = new IntWritable();
                r.next(new Text(), c);
                r.close();
                int count = c.get();
                System.out.println("Count is "+count);
                
                
                
                int i = 1;
                for(; i < 10; i++){
                	// sum the pagerank of dangling nodes with a mapreduce task
                    // Then divide it by the total number of nodes, and add it to everything in the next step
                	
                		
                	
                        String previous = dir + "tr-" + (i - 1) + ".out";
                        String sumrank = dir + "sr-" + (i -1) + ".out";
                        
                        String[] opts = {previous, sumrank};
                        
                        ToolRunner.run(new Dangling(), opts);
                        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(sumrank+"/part-r-00000"), conf);
                        DoubleWritable dangling = new DoubleWritable();
                        reader.next(new Text(), dangling);
                        reader.close();
                        
                        System.out.println("=============================");
                        System.out.println("Dangling is "+dangling.get());
                        
                        String current = dir + "tr-" + i + ".out";
                        String[] opts2 = {previous, current};
                        // Also pass in missing dangling node stuff from last step, and add it to each pair.
                        ToolRunner.run(new Rank((float) dangling.get(),count), opts2);
                        
                        
                    
                        fs.delete(new Path(previous), true);
                        fs.delete(new Path(sumrank), true);
                        
                }
                String prev = dir + "tr-"+(i-1)+".out";
                
                String[] aggOpts = { prev, dir + "tr-done.out" };
                ToolRunner.run(new SumProjects(), aggOpts);
                
                fs.delete(new Path(prev), true);

                
                // Aggregate all Type names under their respective projects
                // create file containing package names and their respective projects 
                // (mapper just outputs package->project for each)
                // run a multipleinput-reduce that ends up producing project - score numbers
                // run a reduce over those scores to sum them
                
            
                timer.stop();
                System.out.println("Elapsed " + timer.toString());

        }

}

