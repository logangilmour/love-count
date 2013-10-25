package a2;

import java.io.IOException;

import org.apache.commons.io.IOUtils;

public class TryGet {
	public static void main(String args[]){
		ArchiveStringIterator  it = null;
		try {
			it = new ArchiveStringIterator("https://api.github.com/repos/logangilmour/universe/tarball/");
			for(String s: it){
				System.out.println(s);
			}
			

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			IOUtils.closeQuietly(it);
		}
	}
}
