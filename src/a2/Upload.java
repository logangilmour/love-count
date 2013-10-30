package a2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Upload {
	
	public static String HPATH = System.getenv("HADOOP_HOME");
	
	public static void main(String args[]){
		Configuration conf = new Configuration();
		conf.addResource(new Path(HPATH+"conf/core-site.xml"));
		conf.addResource(new Path(HPATH+"conf/hdfs-site.xml"));
		FileSystem fs = null;
		FileInputStream in = null;
		FSDataOutputStream out = null;
		try {
			fs = FileSystem.get(conf);
			File dir = new File(args[0]);
			if(dir.isDirectory()){
				for (File file : dir.listFiles()){
					if(file.isFile()){
						in = new FileInputStream(file);
						out = fs.create(new Path("/libs/"+file.getName()));
						int size = 1024;
						byte[] bytes = new byte[size];
						int read=0;
						while((read = in.read(bytes))>0){
							out.write(bytes,0,read);
						}
					
					}
				}
			}
		} catch (IOException e1) {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(out);
			IOUtils.closeQuietly(fs);
			e1.printStackTrace();
		}
		
	}
}
