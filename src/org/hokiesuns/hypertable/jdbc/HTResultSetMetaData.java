package org.hokiesuns.hypertable.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Allows for the querying of the resultset to get column information. As with the
 * HTDatabaseMetaData class, the column counts and column names reflect the values as
 * defined by the table's schema. While the underlying ResultSet allows the client to request
 * column qualified values, the metadata does not due to the infinitely possible qualified values for
 * each column.
 * 
 * @author Amit Nithian
 * @copyright Amit Nithian 2010
 */
public class HTResultSetMetaData implements ResultSetMetaData {

	private List<String> mColumns;
	public HTResultSetMetaData(List<String> pColumns)
	{
		mColumns = pColumns;
	}
	
	@Override
	public String getCatalogName(int arg0) throws SQLException {
		
		return getColumnName(arg0);
	}

	@Override
	public String getColumnClassName(int arg0) throws SQLException {
		
		return "string";
	}

	@Override
	public int getColumnCount() throws SQLException {
		
		return mColumns.size();
	}

	@Override
	public int getColumnDisplaySize(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public String getColumnLabel(int arg0) throws SQLException {
		
		return mColumns.get(arg0-1);
	}

	@Override
	public String getColumnName(int arg0) throws SQLException {
		
		return getColumnLabel(arg0);
	}

	@Override
	public int getColumnType(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public String getColumnTypeName(int arg0) throws SQLException {
		
		return "string";
	}

	@Override
	public int getPrecision(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public int getScale(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public String getSchemaName(int arg0) throws SQLException {
		
		return "default";
	}

	@Override
	public String getTableName(int arg0) throws SQLException {
		
		return "unknown";
	}

	@Override
	public boolean isAutoIncrement(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public boolean isCaseSensitive(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public boolean isCurrency(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public int isNullable(int arg0) throws SQLException {
		
		return 0;
	}

	@Override
	public boolean isReadOnly(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public boolean isSearchable(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public boolean isSigned(int arg0) throws SQLException {
		
		return false;
	}

	@Override
	public boolean isWritable(int arg0) throws SQLException {
		
		return false;
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
