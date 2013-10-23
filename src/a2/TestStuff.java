package a2;

import java.net.UnknownHostException;

import com.mongodb.DBCursor;

public class TestStuff {
	public static void main(String args[]){
		Fetcher fetcher = new Fetcher();
		
		fetcher.cloneRepo("https://github.com/fzoli/MillServer.git", "/tmp/repo");
		
		
	}
}
