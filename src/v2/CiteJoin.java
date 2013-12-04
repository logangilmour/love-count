package v2;

import java.util.ArrayList;
import java.util.List;


import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

public class CiteJoin {
	

        public static void main(String[] args) throws Exception {
                
                StopWatch timer = new StopWatch();
                timer.start();
                Configuration conf = new Configuration();     
                FileSystem fs = FileSystem.get(conf);

                
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                
                String dir = otherArgs[1];
                
                // first, count all of the things to set the initial value


                
                // Then, set each rank up to be that initial value
                String[] initOpts = { otherArgs[0], dir + "ir-0.out" };
                ToolRunner.run(new InitRank(), initOpts);
                
                String[] groupOpts = { dir + "ir-0.out", dir + "tr-0.out"};
                ToolRunner.run(new Aggregate(), groupOpts);
                
                String[] aggOpts = { dir + "tr-0.out", dir + "tr-done.out" };
                ToolRunner.run(new PrintProjects(), aggOpts);
                

                
                // Aggregate all Type names under their respective projects
                // create file containing package names and their respective projects 
                // (mapper just outputs package->project for each)
                // run a multipleinput-reduce that ends up producing project - score numbers
                // run a reduce over those scores to sum them
                
            
                timer.stop();
                System.out.println("Elapsed " + timer.toString());

        }

}

