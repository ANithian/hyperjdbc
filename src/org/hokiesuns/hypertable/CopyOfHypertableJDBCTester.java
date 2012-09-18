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
public class CopyOfHypertableJDBCTester {

	public static void main(String[] args) throws Exception
	{
		new HTDriver();
		System.out.println("Getting new connection.");
		Connection conn = DriverManager.getConnection("jdbc:hypertable://zap1.admin.zvents.com:38080//");
		Statement stmt = conn.createStatement();
		
		DatabaseMetaData metaData = conn.getMetaData();
		ResultSet rs = metaData.getTables("", "", "", null);
		System.out.println("Listing tables...");
		while(rs.next())
		{
			System.out.println(rs.getString("TABLE_NAME"));
		}

		rs = stmt.executeQuery("select * from product_features limit 10");
		while(rs.next())
		{
			System.out.println("Current Row Columns = " + ((HTResultSet)rs).getCurrentColumnList());
		}

	}
}
