package com.hashMapData;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ParseSQL {
	
	public String rawQuery = null;
	public List<String> queryList = null;
	public Connection conn = null;
	
	public void init(){
		try {
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
			conn = DriverManager.getConnection("jdbc:hsqldb:hsql://172.29.32.64:9001/NRMS1", "SA", "");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public ArrayList<String> parseSQL(InputStream input) throws IOException{
		init();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		queryList = new ArrayList<String>();		
		
		while(reader.readLine() != null){
			rawQuery = reader.readLine();
			queryList.add(rawQuery);
			//System.out.println(rawQuery);
		}
		return (ArrayList<String>) queryList;
	}
	
	public void insertIntoDB(InputStream input){
		//ResultSet rs = null;
		int count=0;
		Statement st = null;
		try {
			ArrayList<String> list = parseSQL(input);
			Iterator<String> itr = list.iterator();
			st = conn.createStatement();
			
			while(itr.hasNext()){
				//System.out.println(itr.next());
				if(count>30212)
				st.execute(itr.next());
				count++;
				System.out.println(count);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		ParseSQL parse = new ParseSQL();
		try {
			FileInputStream str = new FileInputStream("D:\\ftp\\"+"opm_usr_clnt_cntr.sql");
			parse.insertIntoDB(str);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
