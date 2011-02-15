package org.hokiesuns.hypertable.jdbc;

import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Hypertable JDBC driver that must be loaded in the classloader (like any other JDBC driver)
 * to perform HQL Queries.
 * 
 * @author Amit Nithian
 * @copyright Amit Nithian 2010
 */
public class HTDriver implements Driver {

	static
	{
		HTDriver driver = new HTDriver();
		try {
			DriverManager.registerDriver(driver);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	/**
	 * Hypertable URLs look like:
	 * jdbc:hypertable://host:port where host and port are the host/port of the Thrift server.
	 */
	public boolean acceptsURL(String arg0) throws SQLException {
		
		return arg0 != null && arg0.contains("hypertable:");
	}

	@Override
	/**
	 * This creates a new connection to hypertable. Since 0.9.4, namespaces are 
	 * supported and to support aboslute vs relative namespace, if the root
	 * namespace begins with '/', then you must specify a '//'. Example:
	 * jdbc:hypertable://localhost:38080//data/qa ==> '/data/qa' namespace
	 * 
	 */
	public Connection connect(String arg0, Properties arg1) throws SQLException {
		
		HTConnection conn = null;
		try
		{
			String sConnUrl = arg0.replace("jdbc:", "");
			URI u = new URI(sConnUrl);
			//jdbc:hypertable://host:port/namespace
			conn = new HTConnection(u.getHost(), u.getPort(),u.getPath());
		}
		catch(Exception e)
		{
			throw new SQLException(e);
		}
		return conn;
	}

	@Override
	public int getMajorVersion() {
		
		return 0;
	}

	@Override
	public int getMinorVersion() {
		
		return 1;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public boolean jdbcCompliant() {
		
		return false;
	}

}
