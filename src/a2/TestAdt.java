package a2;

import japa.parser.JavaParser;
import japa.parser.ParseException;
import japa.parser.ast.ImportDeclaration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

 
public class TestAdt {
	public static void main(String args[]) throws ParseException, IOException{
		ByteArrayInputStream in = new ByteArrayInputStream("package test; import com.awt.*; public class Test {}".getBytes("UTF-8"));
		japa.parser.ast.CompilationUnit unit;
		try {
			unit = JavaParser.parse(in);
			
		}finally{
			in.close();
		}
		System.out.println("Contained in "+unit.getPackage().getName()+":");
		for(ImportDeclaration dec: unit.getImports()){
			System.out.println(dec.toString());
		}
	}
}
