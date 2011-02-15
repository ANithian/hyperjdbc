package org.hokiesuns.hypertable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.hokiesuns.hypertable.jdbc.HTDriver;
import org.hokiesuns.hypertable.jdbc.HTResultSet;

/**
 * Simple Hypertable JDBC tester to show the functionality of the driver.
 * 
 * @author Amit Nithian
 * @copyright Amit Nithian 2010
 */
public class HypertableJDBCTester {

	public static void main(String[] args) throws Exception
	{
		if(args.length < 1)
		{
			System.out.println("Usage: HypertableJDBCTester <HTHost>");
			System.exit(1);
		}
		
		new HTDriver();
		System.out.println("Getting new connection.");
		Connection conn = DriverManager.getConnection("jdbc:hypertable://" + args[0] + ":38080//");
		Statement stmt = conn.createStatement();
		
		DatabaseMetaData metaData = conn.getMetaData();
		ResultSet rs = metaData.getTables("", "", "", null);
		System.out.println("Listing tables...");
		while(rs.next())
		{
			System.out.println(rs.getString("TABLE_NAME"));
		}
		
		System.out.println("Creating a new table user_table for testing");
		stmt.execute("drop table if exists user_table");
		if(stmt.execute("create table user_table (name, address, phone)"))
			System.out.println("Succesfully created user_table");
		System.out.println("Inserting a few users.");
		if(stmt.execute("insert into user_table values ('1234','name','Amit')," +
				"('1234','phone','222-222-1234'),('1234','address','11 Some Street')," +
				"('4567','name','Charlie Brown'),('4567','address:home','12 Another Street'),('4567','address:work','1800 Another Street'),('4567','phone','510-222-1234')"))
			System.out.println("Added a few rows.");
		rs = stmt.executeQuery("select * from user_table");
		while(rs.next())
		{
			System.out.println("Current Row Columns = " + ((HTResultSet)rs).getCurrentColumnList());
			System.out.println("RowKey=" + rs.getString(HTResultSet.ROW));
			System.out.println("Name=" + rs.getString("name"));
			System.out.println("Address: " + rs.getString("address"));
			System.out.println("Home Address: " + rs.getString("address:home"));
			System.out.println("Work Address: " + rs.getString("address:work"));
			System.out.println("Phone Number: " + rs.getString("phone"));
		}
		
		System.out.println("Dropping table");
		if(stmt.execute("drop table user_table"))
			System.out.println("Succesfully dropped user_table");
	}
}
