package org.hokiesuns.hypertable.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hypertable.thriftgen.Cell;

/**
 * Implementation of ResultSet that provides all the necessary methods to retrieve both
 * column values and column qualifier values if present. For example,
 * rs.getString("address") will return the address of the row if set. Likewise, rs.getString("address:home") will
 * return the home qualified address if set; however, rs.getString("address") will <i>not</i> return all the qualified values associated
 * with the column "address".
 * 
 * Most of the methods return the expected results. The first two columns are always ROW and TIMESTAMP so the first user defined column's index
 * is 3.
 * @author Amit Nithian
 * @copyright Amit Nithian 2010
 */
public class HTResultSet implements ResultSet {

//	private HqlResult mHqlResult;
	private Iterator<Cell> mCellIterator;
	
	private Map<String,String> mCurrentRow= new HashMap<String, String>();
//	private Map<Integer,String> mColumnPositions = new HashMap<Integer, String>();
	
	public static final String ROW="row";
	public static final String TIMESTAMP="timestamp";
	private String m_sCurrentRowKey = "";
	private Cell mCurrentCell = null;
	private int m_iRowNumber = 0;
	private boolean m_bLastColumnReadWasNull=false;
	private boolean m_bIsLast = false;
	private Statement mStatement = null;
	private List<String> mColumns = null;
	private List<String> mCurrentRowColumns = new ArrayList<String>();
	
	private ResultSetMetaData mResultSetMeta = null;
	
	public HTResultSet(Iterator<Cell> pCellIterator, Statement pStatement, List<String> pColumnList) throws SQLException
	{
//		mHqlResult = pResult;
		mCellIterator = pCellIterator;
		mStatement = pStatement;
		mColumns = pColumnList;

		mResultSetMeta = new HTResultSetMetaData(pColumnList);
//		next();
	}
	
	public List<String> getCurrentColumnList()
	{
		return mCurrentRowColumns;
	}
	@Override
	public boolean absolute(int arg0) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Hypertable driver doesn't support random access");
	}

	@Override
	public void afterLast() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hypertable driver doesn't support cursor movement");

	}

	@Override
	public void beforeFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hypertable driver doesn't support cursor movement");

	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void clearWarnings() throws SQLException {
		

	}

	@Override
	public void close() throws SQLException {
		

	}

	@Override
	public void deleteRow() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Delete rows using the DELETE HQL statement.");
	}

	@Override
	public int findColumn(String arg0) throws SQLException {
		
//		for(Entry<Integer, String> e:mColumns)
		int iColumn = mColumns.indexOf(arg0);
		if(iColumn < 0)
			throw new SQLException(arg0 + " is not a valid column.");
		return iColumn;
	}

	@Override
	public boolean first() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Hypertable driver doesn't support random access");
	}

	@Override
	public Array getArray(int arg0) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Array Type doesn't exist in HyperTable");
	}

	@Override
	public Array getArray(String arg0) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Array Type doesn't exist in HyperTable");
	}

	@Override
	public InputStream getAsciiStream(int arg0) throws SQLException {
		
		String sColName = mColumns.get(arg0-1);
		if(sColName==null)
			throw new SQLException(arg0 + " is not a valid column index.");
		return getAsciiStream(sColName);
	}

	@Override
	public InputStream getAsciiStream(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
		{
			try {
				return new ByteArrayInputStream(val.getBytes("US-ASCII"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				throw new SQLException(e);
			}
		}
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return new BigDecimal(val);
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return new BigDecimal(val);
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return new BigDecimal(val).setScale(arg1);
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return new BigDecimal(val).setScale(arg1);
		return null;
	}

	@Override
	public InputStream getBinaryStream(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
		{
			return new ByteArrayInputStream(val.getBytes());
		}
		return null;
	}

	@Override
	public InputStream getBinaryStream(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
		{
			return new ByteArrayInputStream(val.getBytes());
		}
		return null;
	}

	@Override
	public Blob getBlob(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Blob getBlob(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public boolean getBoolean(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
		{
			return Boolean.parseBoolean(val);
		}
		return false;
	}

	@Override
	public boolean getBoolean(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
		{
			return Boolean.parseBoolean(val);
		}
		return false;
	}

	@Override
	public byte getByte(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
		{
			return Byte.parseByte(val);
		}
		return 0;
	}

	@Override
	public byte getByte(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
		{
			return Byte.parseByte(val);
		}
		return 0;
	}

	@Override
	public byte[] getBytes(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
		{
			return val.getBytes();
		}
		return null;
	}

	@Override
	public byte[] getBytes(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
		{
			return val.getBytes();
		}
		return null;
	}

	@Override
	public Reader getCharacterStream(int arg0) throws SQLException {
		
		return getNCharacterStream(arg0);
	}

	@Override
	public Reader getCharacterStream(String arg0) throws SQLException {
		
		return getNCharacterStream(arg0);
	}

	@Override
	public Clob getClob(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Clob getClob(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public int getConcurrency() throws SQLException {
		
		return 0;
	}

	@Override
	public String getCursorName() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Not supported.");
	}

	@Override
	public Date getDate(int arg0) throws SQLException {
		
		return new Date(getLong(arg0));
	}

	@Override
	/**
	 * NOTE: Assumes that the underlying type is a long data type that contains a timestamp value. Does
	 * not do date parsing on strings (for now due to date format/locale issues).
	 */
	public Date getDate(String arg0) throws SQLException {
		
		return new Date(getLong(arg0));
	}

	@Override
	public Date getDate(int arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public Date getDate(String arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public double getDouble(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return Double.parseDouble(val);
		return 0;
	}

	@Override
	public double getDouble(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return Double.parseDouble(val);
		return 0;
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
	public float getFloat(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return Float.parseFloat(val);
		return 0;
	}

	@Override
	public float getFloat(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return Float.parseFloat(val);
		return 0;
	}

	@Override
	public int getHoldability() throws SQLException {
		
		return 0;
	}

	@Override
	public int getInt(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return Integer.parseInt(val);
		return 0;
	}

	@Override
	public int getInt(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return Integer.parseInt(val);
		return 0;
	}

	@Override
	public long getLong(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return Long.parseLong(val);
		return 0;
	}

	@Override
	public long getLong(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return Long.parseLong(val);
		return 0;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		
//		return new HTResultSetMetaData(mColumnPositions);
		return mResultSetMeta;
//		return null;
	}

	@Override
	public Reader getNCharacterStream(int arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return new StringReader(val);
		return null;
	}

	@Override
	public Reader getNCharacterStream(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return new StringReader(val);
		return null;
	}

	@Override
	public NClob getNClob(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public NClob getNClob(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public String getNString(int arg0) throws SQLException {
		
		return getString(arg0);
	}

	@Override
	public String getNString(String arg0) throws SQLException {
		
		return getString(arg0);
	}

	@Override
	public Object getObject(int arg0) throws SQLException {
		
		return getString(arg0);
	}

	@Override
	public Object getObject(String arg0) throws SQLException {
		
		return getString(arg0);
	}

	@Override
	public Object getObject(int arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public Object getObject(String arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public Ref getRef(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Ref getRef(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public int getRow() throws SQLException {
		
		return m_iRowNumber;
	}

	@Override
	public RowId getRowId(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public RowId getRowId(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public SQLXML getSQLXML(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public SQLXML getSQLXML(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public short getShort(int arg0) throws SQLException 
	{
		String val = getString(arg0);
		if(val != null)
			return Short.parseShort(val);
		return 0;
	}

	@Override
	public short getShort(String arg0) throws SQLException {
		
		String val = getString(arg0);
		if(val != null)
			return Short.parseShort(val);
		return 0;
	}

	@Override
	public Statement getStatement() throws SQLException {
		
		return mStatement;
	}

	@Override
	public String getString(int arg0) throws SQLException {
		
		String sColumn = mColumns.get(arg0-1);
		return getString(sColumn);
		
//		Integer iMappedColumn = mSchemaColumnToRowColumn.get(arg0);
//		if(iMappedColumn != null)
//		{
//			String sColName = mColumns.get(iMappedColumn-1);
//			return getString(sColName);
//		}
//		return null;
//		if(sColName != null)
//		{
//			
//		}
//		else
//			throw new SQLException(arg0 + " is not a valid column label.");
	}

	@Override
	public String getString(String arg0) throws SQLException 
	{
		//The arg0 could be column or column:family. We know the columns
		//because of the schema so we can at least throw an exception
		//if the column specified is not valid with respect to the schema.
		String sColumn=arg0;
		if(arg0 != null)
		{
			int iColonLoc = arg0.indexOf(':');
			if(iColonLoc >=0)
			{
				sColumn = arg0.substring(0,iColonLoc);
			}
		}
		if(mColumns.indexOf(sColumn) < 0)
			throw new SQLException(arg0 + " is not a valid column label.");
		String sReturn = mCurrentRow.get(arg0);
		m_bLastColumnReadWasNull = sReturn==null;
		
		return sReturn;
	}

	@Override
	public Time getTime(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Time getTime(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Time getTime(int arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public Time getTime(String arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(String arg0, Calendar arg1)
			throws SQLException {
		
		return null;
	}

	@Override
	public int getType() throws SQLException {
		
		return 0;
	}

	@Override
	public URL getURL(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public URL getURL(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public InputStream getUnicodeStream(int arg0) throws SQLException {
		
		return null;
	}

	@Override
	public InputStream getUnicodeStream(String arg0) throws SQLException {
		
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		
		return null;
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Use the INSERT HQL statement to insert data.");

	}

	@Override
	public boolean isAfterLast() throws SQLException {
		
		return mCellIterator.hasNext();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		
		return mCurrentCell==null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		
		return m_iRowNumber == 1;
	}

	@Override
	public boolean isLast() throws SQLException {
		
		return m_bIsLast;
	}

	@Override
	public boolean last() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Hypertable driver doesn't support random access");
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Hypertable driver doesn't support random access");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Hypertable driver doesn't support random access");
	}

	@Override
	public boolean next() throws SQLException 
	{
		if(mCellIterator.hasNext())
		{
			if(mCurrentCell == null)
			{
				mCurrentCell = mCellIterator.next();
			}
			m_sCurrentRowKey = mCurrentCell.key.row;
			m_iRowNumber++;
			mCurrentRow.clear();
			mCurrentRowColumns.clear();
			mCurrentRowColumns.add(ROW);
			mCurrentRowColumns.add(TIMESTAMP);			
			while(mCurrentCell.key.row.equals(m_sCurrentRowKey))
			{
				populateRow();
				if(mCellIterator.hasNext())
					mCurrentCell = mCellIterator.next();
				else
				{
					mCurrentCell=null;
					break;
				}
			}
			return true;
		}
		//One row cell
		if(mCurrentCell != null)
		{
			m_iRowNumber++;
			mCurrentRow.clear();
			mCurrentRowColumns.clear();
			mCurrentRowColumns.add(ROW);
			mCurrentRowColumns.add(TIMESTAMP);			
			populateRow();
			mCurrentCell=null;
			return true;
		}
		if(!m_bIsLast)
			m_bIsLast=true;
		return false;
	}
	
	private void populateRow()
	{
		m_bLastColumnReadWasNull=false;
		
		mCurrentRow.put(ROW, mCurrentCell.key.row);
		mCurrentRow.put(TIMESTAMP, mCurrentCell.key.timestamp + "");
		
		String sColumnName = mCurrentCell.key.column_family;
		String sColumnQualifier = mCurrentCell.key.column_qualifier;
		String sFullyQualifiedColumn = sColumnName;
		if(sColumnQualifier != null && sColumnQualifier.length() > 0)
		{
			sFullyQualifiedColumn = sColumnName + ":" + sColumnQualifier;
		}
		if(!mCurrentRow.containsKey(sFullyQualifiedColumn))
		{
			mCurrentRowColumns.add(sFullyQualifiedColumn);
			String sVal = null;
			if(mCurrentCell.value != null)
				sVal = new String(mCurrentCell.getValue());
			mCurrentRow.put(sFullyQualifiedColumn,sVal);
		}
	}

	@Override
	public boolean previous() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Hypertable resultset forward only.");
	}

	@Override
	public void refreshRow() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Refresh not available in HyperTable ResultSet");
	}

	@Override
	public boolean relative(int arg0) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Hypertable driver doesn't support random access");
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		
		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		
		return false;
	}

	@Override
	public void setFetchDirection(int arg0) throws SQLException {
		

	}

	@Override
	public void setFetchSize(int arg0) throws SQLException {
		

	}

	@Override
	public void updateArray(int arg0, Array arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateArray(String arg0, Array arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1, int arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateAsciiStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateAsciiStream(String arg0, InputStream arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBigDecimal(String arg0, BigDecimal arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1, int arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBinaryStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBinaryStream(String arg0, InputStream arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBlob(int arg0, Blob arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBlob(String arg0, Blob arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBlob(int arg0, InputStream arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBlob(String arg0, InputStream arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBlob(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBlob(String arg0, InputStream arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBoolean(int arg0, boolean arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBoolean(String arg0, boolean arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateByte(int arg0, byte arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateByte(String arg0, byte arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBytes(int arg0, byte[] arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateBytes(String arg0, byte[] arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1, int arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1, int arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateCharacterStream(String arg0, Reader arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateClob(int arg0, Clob arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateClob(String arg0, Clob arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateClob(int arg0, Reader arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateClob(String arg0, Reader arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateClob(int arg0, Reader arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateClob(String arg0, Reader arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateDate(int arg0, Date arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateDate(String arg0, Date arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateDouble(int arg0, double arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateDouble(String arg0, double arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateFloat(int arg0, float arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateFloat(String arg0, float arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateInt(int arg0, int arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateInt(String arg0, int arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateLong(int arg0, long arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateLong(String arg0, long arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNCharacterStream(int arg0, Reader arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNCharacterStream(String arg0, Reader arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNCharacterStream(String arg0, Reader arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNClob(int arg0, NClob arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNClob(String arg0, NClob arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNClob(int arg0, Reader arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNClob(String arg0, Reader arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNClob(int arg0, Reader arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNClob(String arg0, Reader arg1, long arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNString(int arg0, String arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNString(String arg0, String arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNull(int arg0) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateNull(String arg0) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateObject(int arg0, Object arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateObject(String arg0, Object arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateObject(int arg0, Object arg1, int arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateObject(String arg0, Object arg1, int arg2)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateRef(int arg0, Ref arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateRef(String arg0, Ref arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateRow() throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateRowId(int arg0, RowId arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateRowId(String arg0, RowId arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateShort(int arg0, short arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateShort(String arg0, short arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateString(int arg0, String arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateString(String arg0, String arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateTime(int arg0, Time arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateTime(String arg0, Time arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public void updateTimestamp(String arg0, Timestamp arg1)
			throws SQLException {
		
		throw new SQLFeatureNotSupportedException("Update not supported by Hypertable. Consider re-inserting");
	}

	@Override
	public boolean wasNull() throws SQLException {
		
		return m_bLastColumnReadWasNull;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		
		return null;
	}

}
