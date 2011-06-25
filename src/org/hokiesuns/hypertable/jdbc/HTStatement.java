package org.hokiesuns.hypertable.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.thrift.TException;
import org.hokiesuns.hypertable.CellsIterator;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.HqlResult;
import org.hypertable.thriftgen.Schema;

/**
 * Encapsulation of the Thrift execution of HQL queries. Only one ResultSet can be
 * returned per query and features such as fetch-size modification aren't supported.
 * @author Amit Nithian
 * @copyright Amit Nithian 2010
 */
public class HTStatement implements Statement {

	private ThriftClient mHtClient;
	private Connection mHtConnection;
	private ResultSet mCurrentResultSet = null;
	private long mCurrentNamespace = 0;
	
	public HTStatement(ThriftClient pClient, Connection pConn, long pNameSpace)
	{
		mHtConnection = pConn;
		mHtClient = pClient;
		mCurrentNamespace = pNameSpace;
	}
	
	@Override
	public void addBatch(String arg0) throws SQLException {
		

	}

	@Override
	public void cancel() throws SQLException {
		

	}

	@Override
	public void clearBatch() throws SQLException {
		

	}

	@Override
	public void clearWarnings() throws SQLException {
		

	}

	@Override
	public void close() throws SQLException {
		

	}

	@Override
	public boolean execute(String arg0) throws SQLException {
		
		executeQuery(arg0);
		return mCurrentResultSet != null;
	}

	@Override
	public boolean execute(String arg0, int arg1) throws SQLException {
		
		return execute(arg0);
	}

	@Override
	public boolean execute(String arg0, int[] arg1) throws SQLException {
		
		return execute(arg0);
	}

	@Override
	public boolean execute(String arg0, String[] arg1) throws SQLException {
		
		return execute(arg0);
	}

	@Override
	public int[] executeBatch() throws SQLException {
		
		return null;
	}

	private String getTableName(String pQuery)
	{
		//select __ from <table>
		String sQuery = pQuery.replaceAll("\\s", " ");
		String sTableName = null;
		String[] sWords = sQuery.split(" ");
		boolean bFromFound = false;
		for(String s:sWords)
		{
			if(!bFromFound)
				bFromFound = s.equalsIgnoreCase("from");
			else
			{
				sTableName = s.replaceAll(";","");
				break;
			}
		}
		return sTableName;
	}
	
	@Override
	public ResultSet executeQuery(String arg0) throws SQLException {
		try {
		    //Optimization. If the query contains no "where" clause, then
		    //use a scanner and effectively disregard the 'projection' part of the
		    //query in favor of speed.
		    String sLowerQuery = arg0.toLowerCase();
            //Parse the query for the table being selected and then
            //issue a schema request to populate a list of columns
            String sTableInQuery = getTableName(arg0);
            if(sTableInQuery == null)
                throw new SQLException("Table " + sTableInQuery + " is not valid.");
            List<String> lColumns = null;
            
            HqlResult result = mHtClient.hql_exec(mCurrentNamespace, arg0.trim(), false, true);
            
            Iterator<Cell> cellIterator = new CellsIterator(mHtClient, result.scanner);
            if(cellIterator.hasNext())
            {
                lColumns = getTableColumns(sTableInQuery);
                mCurrentResultSet = new HTResultSet(cellIterator, this, lColumns);
            }
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new SQLException("Error executing " + arg0,e);
		}
		return mCurrentResultSet;
	}

	private List<String> getTableColumns(String pTableName) throws ClientException, TException
	{
        Schema tableSchema = mHtClient.get_schema(mCurrentNamespace,pTableName);
        List<String> lColumns = new ArrayList<String>();
        lColumns.add(HTResultSet.ROW);
        lColumns.add(HTResultSet.TIMESTAMP);
        
        lColumns.addAll(tableSchema.column_families.keySet());
        return lColumns;
	}
	@Override
	public int executeUpdate(String arg0) throws SQLException {
		
		execute(arg0);
		return 0;
	}

	@Override
	public int executeUpdate(String arg0, int arg1) throws SQLException {
		
		execute(arg0);
		return 0;
	}

	@Override
	public int executeUpdate(String arg0, int[] arg1) throws SQLException {
		
		execute(arg0);
		return 0;
	}

	@Override
	public int executeUpdate(String arg0, String[] arg1) throws SQLException {
		
		execute(arg0);
		return 0;
	}

	@Override
	public Connection getConnection() throws SQLException {
		
		return mHtConnection;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		
		return 0;
	}

	@Override
	public int getFetchSize() throws SQLException {
		
		return 0;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		
		return null;
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxRows() throws SQLException {
		
		return 0;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		
		return false;
	}

	@Override
	public boolean getMoreResults(int arg0) throws SQLException {
		
		return getMoreResults();
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		
		return 0;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		
		return mCurrentResultSet;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		
		return 0;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		
		return 0;
	}

	@Override
	public int getResultSetType() throws SQLException {
		
		return 0;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		
		return -1;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		
		return false;
	}

	@Override
	public void setCursorName(String arg0) throws SQLException {
		

	}

	@Override
	public void setEscapeProcessing(boolean arg0) throws SQLException {
		

	}

	@Override
	public void setFetchDirection(int arg0) throws SQLException {
		

	}

	@Override
	public void setFetchSize(int arg0) throws SQLException {
		

	}

	@Override
	public void setMaxFieldSize(int arg0) throws SQLException {
		

	}

	@Override
	public void setMaxRows(int arg0) throws SQLException {
		

	}

	@Override
	public void setPoolable(boolean arg0) throws SQLException {
		

	}

	@Override
	public void setQueryTimeout(int arg0) throws SQLException {
		

	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		
		return null;
	}

}
