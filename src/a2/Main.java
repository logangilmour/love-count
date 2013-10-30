package a2;

import mapred.Aggregate;
import mapred.InitRank;
import mapred.Rank;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

                String[] initOpts = { otherArgs[0], dir + "tr-0.out" };
                ToolRunner.run(new InitRank(), initOpts);
                
                
                
                
                
                int i = 1;
                for(; i < 10; i++){
                        String previous = dir + "tr-" + (i - 1) + ".out";
                        String current = dir + "tr-" + i + ".out";
                        String[] opts = {previous, current};
                        ToolRunner.run(new Rank(), opts);
                        fs.delete(new Path(previous), true);
                }
                String prev = dir + "tr-"+(i-1)+".out";
                
                String[] aggOpts = { prev, dir + "tr-done.out" };
                ToolRunner.run(new Aggregate(), aggOpts);
                
                fs.delete(new Path(prev), true);

            
                timer.stop();
                System.out.println("Elapsed " + timer.toString());

        }

}

