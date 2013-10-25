package a2;

public class TestStuff {
	public static void main(String args[]){
		Fetcher fetcher = new Fetcher();
		
		fetcher.cloneRepo("https://github.com/fzoli/MillServer.git", "/tmp/repo");
		
		
	}
}
