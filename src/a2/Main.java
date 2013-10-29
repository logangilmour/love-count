package a2;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

public class Main {
	

        public static void main(String[] args) throws Exception {
                
                StopWatch timer = new StopWatch();
                timer.start();
                Configuration conf = new Configuration();                
                
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
                        //fs.delete(new Path(previous), true);
                }
                
                String[] aggOpts = { dir + "tr-"+(i-1)+".out", dir + "tr-done.out" };
                ToolRunner.run(new Aggregate(), aggOpts);
            
                timer.stop();
                System.out.println("Elapsed " + timer.toString());

        }

}

