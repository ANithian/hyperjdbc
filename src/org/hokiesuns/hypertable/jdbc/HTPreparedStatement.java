package org.hokiesuns.hypertable.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.hypertable.thrift.ThriftClient;

public class HTPreparedStatement extends HTStatement implements
        PreparedStatement
{
    private String mHqlQuery = null;
    public HTPreparedStatement(ThriftClient pClient, Connection pConn,
            long pNameSpace, String pQuery)
    {
        super(pClient, pConn, pNameSpace);
        // TODO Auto-generated constructor stub
        mHqlQuery = pQuery;
    }

    @Override
    public void addBatch() throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearParameters() throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean execute() throws SQLException
    {
        // TODO Auto-generated method stub
        return super.execute(mHqlQuery);
    }

    @Override
    public ResultSet executeQuery() throws SQLException
    {
        // TODO Auto-generated method stub
        return super.executeQuery(mHqlQuery);
    }

    @Override
    public int executeUpdate() throws SQLException
    {
        // TODO Auto-generated method stub
        return super.executeUpdate(mHqlQuery);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setArray(int arg0, Array arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, int arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, long arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, int arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, long arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBlob(int arg0, Blob arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBlob(int arg0, InputStream arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBlob(int arg0, InputStream arg1, long arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBoolean(int arg0, boolean arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setByte(int arg0, byte arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBytes(int arg0, byte[] arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, int arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, long arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setClob(int arg0, Clob arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setClob(int arg0, Reader arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setClob(int arg0, Reader arg1, long arg2) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setDate(int arg0, Date arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setDouble(int arg0, double arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setFloat(int arg0, float arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setInt(int arg0, int arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setLong(int arg0, long arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNCharacterStream(int arg0, Reader arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNCharacterStream(int arg0, Reader arg1, long arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNClob(int arg0, NClob arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNClob(int arg0, Reader arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNString(int arg0, String arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNull(int arg0, int arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNull(int arg0, int arg1, String arg2) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setObject(int arg0, Object arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setObject(int arg0, Object arg1, int arg2) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setObject(int arg0, Object arg1, int arg2, int arg3)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setRef(int arg0, Ref arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setRowId(int arg0, RowId arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setSQLXML(int arg0, SQLXML arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setShort(int arg0, short arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setString(int arg0, String arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTime(int arg0, Time arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimestamp(int arg0, Timestamp arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setURL(int arg0, URL arg1) throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void setUnicodeStream(int arg0, InputStream arg1, int arg2)
            throws SQLException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public String toString()
    {
        // TODO Auto-generated method stub
        return mHqlQuery;
    }

}
