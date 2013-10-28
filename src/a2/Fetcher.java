package a2;

import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.javatuples.Pair;


import au.com.bytecode.opencsv.CSVReader;

public class Fetcher {
		
	
	public static String HPATH = "/usr/local/Cellar/hadoop/1.2.1/libexec/";
	//public static String HPATH = "/home/ubuntu/hadoop/";
	public static String DEST = "/default.seq";

	public Fetcher (){
	}
	
	
	public static void main(String [] args){
		DEST=args[0];

		Fetcher fetcher = new Fetcher();
		fetcher.fetch();
		
	}
	

	
	
	public void fetch(){
		CSVReader reader = null;
		try{
		reader = new CSVReader(new FileReader("projects.csv"));
	    String [] nextLine;
	    int i = 0;
	    reader.readNext();
	    final LinkedBlockingQueue<Pair<String,String>> urlQueue = new LinkedBlockingQueue<Pair<String,String>>(20);
	    final LinkedBlockingQueue<Pair<String,String>> writeQueue = new LinkedBlockingQueue<Pair<String,String>>(10);
	    
	    
	    
	    HWriter writer = new HWriter(writeQueue);
	    FetchPool pool = new FetchPool(8,urlQueue,writeQueue);
	
	    
	    while ((nextLine = reader.readNext()) != null) {
	    	i++;	

	    	String id = nextLine[0];
	        String url = nextLine[1];
	        try{
	        	System.out.println("Queueing #"+i);
	        	urlQueue.put(new Pair<String, String>(id, url)); 	
	        } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(System.err);
			}
	    	
	    }

	    pool.shutDown();
	    System.out.println("All readers done");
	    writer.shutdown();
		}catch(IOException e){
			e.printStackTrace(System.err);
		}finally{
			IOUtils.closeQuietly(reader);
		}
	    System.out.println("Main thread shut down.");

	}
}
class FetchPool {
	LinkedList<FetchThread> threads = new LinkedList<FetchThread>();
	public FetchPool(int numThreads,LinkedBlockingQueue<Pair<String,String>> in, LinkedBlockingQueue<Pair<String,String>> out){
		for(int i=0; i< numThreads;i++){
			threads.add(new FetchThread(in,out));
		}
	}
	public void shutDown(){
		for(FetchThread thread : threads){
			thread.shutDown();
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("Sent shutdown to all reader threads.");
	}
}
class FetchThread extends Thread{
	private boolean shutdown = false;
	private final LinkedBlockingQueue<Pair<String,String>> in;
	private final LinkedBlockingQueue<Pair<String,String>> out;
	public FetchThread(LinkedBlockingQueue<Pair<String,String>> in, LinkedBlockingQueue<Pair<String,String>> out){
		this.in=in;
		this.out=out;
		this.start();
	}
	
	public void shutDown(){
		System.out.println("Fetcher shutting down...");
		this.shutdown=true;
		this.interrupt();
		
	}
	public void run(){
		while(!(shutdown && this.in.isEmpty())){
    	    ArchiveStringIterator it = null;
			try {
				Pair<String,String> urlpair = in.take();
	        	String id = urlpair.getValue0();
	        	String url = urlpair.getValue1();
		        
		        
		        
	        	System.out.println(id +", "+ url);

	        	
	        			it = new ArchiveStringIterator(url+"/tarball/");

	        			
	        			for(String s: it){
	        				if(s!=null){
	        					out.put(new Pair<String,String>(id,s));
	        				}
	        			}

			} catch (InterruptedException e) {
				e.printStackTrace(System.err);
				
			} catch (MalformedURLException e) {
				e.printStackTrace(System.err);
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}finally{
				IOUtils.closeQuietly(it);
			}
		}
		System.out.println("Fetch thread shut down.");
	}
}

class HWriter extends Thread{
	private boolean shutdown = false;

	private final LinkedBlockingQueue<Pair<String,String>> in;
	public HWriter(LinkedBlockingQueue<Pair<String,String>> in){
		this.in=in;
		this.start();
	}
	
	public void run(){
		Configuration conf = new Configuration();
		conf.addResource(new Path(Fetcher.HPATH+"conf/core-site.xml"));
		conf.addResource(new Path(Fetcher.HPATH+"conf/hdfs-site.xml"));
	    Path seqFilePath = new Path(Fetcher.DEST);
		FileSystem fs = null;
        Writer writer = null;
		try {
			fs = FileSystem.get(conf);
			writer = SequenceFile.createWriter(fs,conf,seqFilePath,Text.class,Text.class);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while(!(shutdown && in.isEmpty())){
			
			try {
				Pair<String,String> classPair = in.take();
				writer.append(new Text(classPair.getValue0()), new Text(classPair.getValue1()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(System.err);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(System.err);
			}
		}
		IOUtils.closeQuietly(writer);
	    System.out.println("Writer thread shut down.");

	}
	public void shutdown(){
		this.shutdown=true;
		System.out.println("Writer shutting down...");
		this.interrupt();
	}
};