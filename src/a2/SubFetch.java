package a2;

import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.io.IOUtils;
import org.javatuples.Pair;

import au.com.bytecode.opencsv.CSVReader;

public class SubFetch {
		
	
	public static String HPATH = System.getenv("HADOOP_HOME");

	public static String DEST = "/default.seq";

	public SubFetch (){
	}
	
	
	public static void main(String [] args){
		DEST=args[0];

		SubFetch fetcher = new SubFetch();
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
	    	if(i%10000==0){
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
