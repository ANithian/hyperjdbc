package org.hokiesuns.hypertable.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.ClientException;
import org.hypertable.thriftgen.Schema;

/**
 * Allows user to query Hypertable for metadata information. Column information returned
 * does not include column famliy qualifiers but only those columns defined in the schema.
 *  
 * @author Amit Nithian
 * @copyright Amit Nithian 2010
 */
public class HTDatabaseMetadata implements DatabaseMetaData {

	//Some schema information
	private Map<String, ResultSet> mColumnInformation = new HashMap<String, ResultSet>(); //Map of column resultset by table
//	private Map<String,ResultSet> mTableInformation; //Map of table information resultsets
	private ResultSet mTableRS; //The resultset used by the getSchemas() method
	private ThriftClient mHtClient = null;
	private long mCurrentNamespace;
	
	public HTDatabaseMetadata(ThriftClient pHTClient, long pNamespace) throws TException, ClientException
	{
		mHtClient = pHTClient;
		mCurrentNamespace = pNamespace;		
	}
	
	private void reloadInfo() throws Exception
	{
		List<String> lTables = mHtClient.get_tables(mCurrentNamespace);
		List<Map<String, Object>> tableRows = new ArrayList<Map<String,Object>>();
		
		for(String s:lTables)
		{
			Map<String, Object> tableMap = new LinkedHashMap<String, Object>();
			tableMap.put("TABLE_CAT", null);
			tableMap.put("TABLE_SCHEM", null);
			tableMap.put("TABLE_NAME", s);
			tableMap.put("TABLE_TYPE ", "TABLE");
			tableMap.put("REMARKS ", "A Hypertable Table");
			tableMap.put("TYPE_CAT ", null);
			tableMap.put("TYPE_SCHEM ", null);
			tableMap.put("TYPE_NAME ", null);
			tableMap.put("SELF_REFERENCING_COL_NAME", HTResultSet.ROW);
			tableMap.put("REF_GENERATION",null);
			
			tableRows.add(tableMap);
//			mTableInformation.put(s,new MapResultSet(tableRows));
			mColumnInformation.put(s, populateTableInformation(s, mHtClient.get_schema(mCurrentNamespace,s)));
		}
		
		mTableRS = new MapResultSet(tableRows);
	}
	private ResultSet populateTableInformation(String pTable, Schema pTableSchema)
	{
		List<Map<String, Object>> columnRows = new ArrayList<Map<String,Object>>();
		Set<String> columnFamilies = pTableSchema.getColumn_families().keySet();
		int iColumnPos = 1;
		for(String s:columnFamilies)
		{
			Map<String,Object> columnMap = new LinkedHashMap<String, Object>();
			columnMap.put("TABLE_CAT", null);
			columnMap.put("TABLE_SCHEM", null);
			columnMap.put("TABLE_NAME", pTable);
			columnMap.put("COLUMN_NAME", s);
			columnMap.put("DATA_TYPE", Types.VARCHAR);
			columnMap.put("TYPE_NAME", "String");
			columnMap.put("COLUMN_SIZE",-1);
			columnMap.put("BUFFER_LENGTH", null);
			columnMap.put("DECIMAL_DIGITS", null);
			columnMap.put("NUM_PREC_RADIX",10);
			columnMap.put("NULLABLE",DatabaseMetaData.columnNullable);
			columnMap.put("REMARKS","Hypertable Column Family");
			columnMap.put("COLUMN_DEF","NULL");
			columnMap.put("SQL_DATA_TYPE",null);
			columnMap.put("SQL_DATETIME_SUB",null);
			columnMap.put("CHAR_OCTET_LENGTH",null);
			columnMap.put("ORDINAL_POSITION",iColumnPos);
			columnMap.put("IS_NULLABLE","NULLABLE");
			columnMap.put("SCOPE_CATLOG",null);
			columnMap.put("SCOPE_SCHEMA",null);
			columnMap.put("SCOPE_TABLE",null);
			columnMap.put("SOURCE_DATA_TYPE",null);
			columnMap.put("IS_AUTOINCREMENT", "NO");
			columnRows.add(columnMap);
			
			iColumnPos++;
		}
		return new MapResultSet(columnRows);
	}
	
	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		
		return false;
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		
		return false;
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern,
			String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) throws SQLException {
		
		return null;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		
		return null;
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, String columnNamePattern) throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		
		try
		{
			reloadInfo();
		}
		catch(Exception e)
		{
			throw new SQLException(e);
		}
		
		ResultSet rs = mColumnInformation.get(tableNamePattern);
		if(rs != null)
			rs.first();
		return rs;
	}

	@Override
	public Connection getConnection() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) throws SQLException {
		
		return null;
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		
		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		
		return 0;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		
		return "Hypertable";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		
		return "0.9";
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		
		return 0;
	}

	@Override
	public int getDriverMajorVersion() {
		
		return 0;
	}

	@Override
	public int getDriverMinorVersion() {
		
		return 0;
	}

	@Override
	public String getDriverName() throws SQLException {
		
		return "Hypertable Driver";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		
		return "0.1";
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table)
			throws SQLException {
		
		return null;
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
			String functionNamePattern, String columnNamePattern)
			throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern,
			String functionNamePattern) throws SQLException {
		
		return null;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) throws SQLException {
		
		return null;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		
		return 0;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		
		return 0;
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern,
			String procedureNamePattern, String columnNamePattern)
			throws SQLException {
		
		return null;
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern,
			String procedureNamePattern) throws SQLException {
		
		return null;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		
		return 0;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		
		return null;
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		
		return null;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		
		return 0;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern)
			throws SQLException {
		
		return null;
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		
		return null;
	}

	@Override
	public String getStringFunctions() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern,
			String typeNamePattern) throws SQLException {
		
		return null;
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
			String tableNamePattern) throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		
		try
		{
			reloadInfo();
		}
		catch(Exception e)
		{
			throw new SQLException(e);
		}
		return mTableRS;
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern,
			String typeNamePattern, int[] types) throws SQLException {
		
		return null;
	}

	@Override
	public String getURL() throws SQLException {
		
		return null;
	}

	@Override
	public String getUserName() throws SQLException {
		
		return null;
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema,
			String table) throws SQLException {
		
		return null;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		
		return false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency)
			throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability)
			throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level)
			throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		
		return false;
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		
		return false;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		
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
