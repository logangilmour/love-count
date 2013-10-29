package a2;

import japa.parser.JavaParser;
import japa.parser.ParseException;
import japa.parser.ast.ImportDeclaration;
import japa.parser.ast.body.TypeDeclaration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

 
public class TestAdt {
	public static void main(String args[]) throws ParseException, IOException{
		ByteArrayInputStream in = new ByteArrayInputStream("package data.pointset; import data.pointset.Test; public class ParseeException extends Exception { private static final long serialVersionUID = -8545550204136045567L; public ParseException(String message){super(message);}} class stupid{}".getBytes("UTF-8"));
		japa.parser.ast.CompilationUnit unit;
		try {
			unit = JavaParser.parse(in);
			
		}finally{
			in.close();
		}
		System.out.println("Contained in "+unit.getPackage().getName()+":");
		for(TypeDeclaration type: unit.getTypes()){
			System.out.println(type.getMembers());
		}
		class uhoh{}
	}
}
