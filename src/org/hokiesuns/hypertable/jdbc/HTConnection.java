package org.hokiesuns.hypertable.jdbc;

import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hypertable Connection implementation providing basic functionality of the Connection
 * interface. Mainly supports getting a new Statement to do HQL queries.
 *  
 * @author Amit Nithian
 * @copyright Amit Nithian 2010
 *
 */
public class HTConnection implements Connection {

	private ThriftClient mHtClient = null;
	private boolean m_bIsClosed = false;
	private long mNameSpace = 0;
	private static Logger sm_log = LoggerFactory.getLogger(HTConnection.class);
	
	public HTConnection(String pHost, int pPort, String pNameSpace) throws TException
	{
		mHtClient = ThriftClient.create(pHost, pPort);
		String sNameSpace = pNameSpace;
		
		if(sNameSpace != null && sNameSpace.length() > 0 && sNameSpace.startsWith("/"))
		{
		    sNameSpace = sNameSpace.substring(1);
		}
		try
        {
            mNameSpace = mHtClient.open_namespace(sNameSpace);
        }
        catch (ClientException e)
        {
            // TODO Auto-generated catch block
            throw new TException(e);
        }
	}
	
	@Override
	public void clearWarnings() throws SQLException {
		

	}

	@Override
	public void close() throws SQLException {
		
		mHtClient.close();
		m_bIsClosed = true;
	}

	@Override
	public void commit() throws SQLException {
		

	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		
		return null;
	}

	@Override
	public Clob createClob() throws SQLException {
		
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		
		return null;
	}

	@Override
	public Statement createStatement() throws SQLException {
		
		return new HTStatement(mHtClient,this,mNameSpace);
	}

	@Override
	public Statement createStatement(int arg0, int arg1) throws SQLException {
		
		return new HTStatement(mHtClient,this,mNameSpace);
	}

	@Override
	public Statement createStatement(int arg0, int arg1, int arg2)
			throws SQLException {
		
		return new HTStatement(mHtClient,this,mNameSpace);
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		
		return null;
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		
		return false;
	}

	@Override
	public String getCatalog() throws SQLException {
		
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		
		return null;
	}

	@Override
	public String getClientInfo(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public int getHoldability() throws SQLException {
		
		return 0;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		DatabaseMetaData retVal = null;
		
		try 
		{
			retVal = new HTDatabaseMetadata(mHtClient,mNameSpace);
		} 
		catch (Exception e)
		{
			throw new SQLException(e);
		}
		
		return retVal;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		
		return 0;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return m_bIsClosed;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isValid(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public String nativeSQL(String arg0) throws SQLException {
		
		return arg0;
	}

	@Override
	public CallableStatement prepareCall(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2)
			throws SQLException {
		
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0) throws SQLException {
		
		return new HTPreparedStatement(mHtClient, this, mNameSpace, arg0);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int[] arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, String[] arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2)
			throws SQLException {
		
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		
		return null;
	}

	@Override
	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		

	}

	@Override
	public void rollback() throws SQLException {
		

	}

	@Override
	public void rollback(Savepoint arg0) throws SQLException {
		

	}

	@Override
	public void setAutoCommit(boolean arg0) throws SQLException {
		

	}

	@Override
	public void setCatalog(String arg0) throws SQLException {
		

	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException {
		

	}

	@Override
	public void setClientInfo(String arg0, String arg1)
			throws SQLClientInfoException {
		

	}

	@Override
	public void setHoldability(int arg0) throws SQLException {
		

	}

	@Override
	public void setReadOnly(boolean arg0) throws SQLException {
		

	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		
		return null;
	}

	@Override
	public Savepoint setSavepoint(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public void setTransactionIsolation(int arg0) throws SQLException {
		

	}

	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		

	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		
		return null;
	}

	public ThriftClient getThriftClient()
	{
	    return mHtClient;
	}
	
	public long getCurrentNameSpace()
	{
	    return mNameSpace;
	}
	
	public String getCellValue(String pTable, String pRow, String pCol)
	throws SQLException
	{
	    ByteBuffer buf;
        try
        {
            buf = mHtClient.get_cell(mNameSpace, pTable, pRow, pCol);
            Cell cTemp = new Cell();
            cTemp.setValue(buf);
            return new String(cTemp.getValue());
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            throw new SQLException(e);
        }
	}
	
	public long getMutator(String pTable, int pFlushInterval) throws SQLException
	{
	    long lReturn = 0;
	    try
	    {
	        lReturn = mHtClient.open_mutator(mNameSpace, pTable, 0, pFlushInterval);
	    }
	    catch(Exception e)
	    {
	        throw new SQLException(e);
	    }
	    return lReturn;
	}
}
