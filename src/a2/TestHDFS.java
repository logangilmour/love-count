package a2;


import java.io.IOException;

public class TestHDFS {
	public static void main(String args[]){
		Fetcher fetcher = new Fetcher();
		try {
			fetcher.initWriter("/test.seq");
			fetcher.writeHDFS("one","first");
			fetcher.writeHDFS("two","Second");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}finally{
			fetcher.closeWriter();
		}
		
	}
}
