package org.tarantool.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.tarantool.JDBCBridge;

@SuppressWarnings("Since15")
public class SQLDatabaseMetadata implements DatabaseMetaData {
    protected static final int _VSPACE = 281;
    protected static final int _VINDEX = 289;
    protected static final int SPACES_MAX = 65535;
    public static final int FLAGS_IDX = 5;
    public static final int FORMAT_IDX = 6;
    public static final int NAME_IDX = 2;
    public static final int INDEX_FORMAT_IDX = 5;
    public static final int SPACE_ID_IDX = 0;
    protected final SQLConnection connection;


    protected class SQLNullResultSet extends SQLResultSet {

        public SQLNullResultSet(JDBCBridge bridge) {
            super(bridge);
        }

        @Override
        protected Object getRaw(int columnIndex) {
            return columnIndex > row.size() ? null : row.get(columnIndex - 1);
        }

        @Override
        protected Integer getColumnIndex(String columnLabel) {
            Integer idx = super.getColumnIndex(columnLabel);
            return idx == null ? Integer.MAX_VALUE : idx;
        }


    }

    public SQLDatabaseMetadata(SQLConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.url;
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.properties.getProperty("user");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return !nullsAreSortedHigh();
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return !nullsAreSortedAtStart();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Tarantool";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return connection.connection.getServerVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return "tarantool-java";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "1.8.jdbc";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 8;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return " ";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
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
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
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
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
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
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
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
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
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
    public boolean supportsSelectForUpdate() throws SQLException {
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
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
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
    public int getMaxBinaryLiteralLength() throws SQLException {
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
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
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
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
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
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    protected boolean like(String value, String[] parts) {
        if (parts == null || parts.length == 0) {
            return true;
        }
        int i = 0;
        for (String part : parts) {
            i = value.indexOf(part, i);
            if (i < 0) {
                break;
            }
            i += part.length();
        }
        return (i > -1 && (parts[parts.length - 1].length() == 0 || i == value.length()));
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        if (types != null && !Arrays.asList(types).contains("TABLE")) {
            return new SQLResultSet(JDBCBridge.EMPTY);
        }
        String[] parts = tableNamePattern == null ? new String[]{""} : tableNamePattern.split("%");
        List<List<Object>> spaces = (List<List<Object>>) connection.connection.select(_VSPACE, 0, Arrays.asList(), 0, SPACES_MAX, 0);
        List<List<Object>> rows = new ArrayList<List<Object>>();
        for (List<Object> space : spaces) {
            String name = (String) space.get(NAME_IDX);
            Map flags = (Map) space.get(FLAGS_IDX);
            if (flags != null && flags.containsKey("sql") && like(name, parts)) {
                rows.add(Arrays.asList(name, "TABLE", flags.get("sql")));
            }
        }
        return new SQLNullResultSet(JDBCBridge.mock(Arrays.asList("TABLE_NAME", "TABLE_TYPE", "REMARKS",
                //nulls
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_TYPE", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"), rows));
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return rowOfNullsResultSet();
    }

    private SQLNullResultSet rowOfNullsResultSet() {
        return new SQLNullResultSet(JDBCBridge.mock(Collections.<String>emptyList(), Collections.singletonList(Collections.emptyList())));
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return rowOfNullsResultSet();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return new SQLResultSet(JDBCBridge.mock(Arrays.asList("TABLE_TYPE"), Arrays.asList(Arrays.<Object>asList("TABLE"))));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        String[] tableParts = tableNamePattern == null ? new String[]{""} : tableNamePattern.split("%");
        String[] colParts = columnNamePattern == null ? new String[]{""} : columnNamePattern.split("%");
        List<List<Object>> spaces = (List<List<Object>>) connection.connection.select(_VSPACE, 0, Arrays.asList(), 0, SPACES_MAX, 0);
        List<List<Object>> rows = new ArrayList<List<Object>>();
        for (List<Object> space : spaces) {
            String tableName = (String) space.get(NAME_IDX);
            Map flags = (Map) space.get(FLAGS_IDX);
            if (flags != null && flags.containsKey("sql") && like(tableName, tableParts)) {
                List<Map<String, Object>> format = (List<Map<String, Object>>) space.get(FORMAT_IDX);
                for (int columnIdx = 1; columnIdx <= format.size(); columnIdx++) {
                    Map<String, Object> f = format.get(columnIdx - 1);
                    String columnName = (String) f.get("name");
                    String dbType = (String) f.get("type");
                    if (like(columnName, colParts)) {
                        rows.add(Arrays.<Object>asList(tableName, columnName, columnIdx, Types.OTHER, dbType, 10, 1, "YES", Types.OTHER, "NO", "NO"));
                    }
                }
            }
        }

        return new SQLNullResultSet((JDBCBridge.mock(
                Arrays.asList("TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "DATA_TYPE", "TYPE_NAME", "NUM_PREC_RADIX", "NULLABLE", "IS_NULLABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN",
                        //nulls
                        "TABLE_CAT", "TABLE_SCHEM", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE"
                ),
                rows)));
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return rowOfNullsResultSet();
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return rowOfNullsResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        List<List<Object>> spaces = (List<List<Object>>) connection.connection.select(_VSPACE, 2, Arrays.asList(table), 0, 1, 0);

        List<List<Object>> rows = new ArrayList<List<Object>>();

        if (spaces != null && spaces.size() > 0) {
            List<Object> space = spaces.get(0);
            List<Map<String, Object>> fields = (List<Map<String, Object>>) space.get(FORMAT_IDX);
            List<List<Object>> indexes = (List<List<Object>>) connection.connection.select(_VINDEX, 0, Arrays.asList(space.get(SPACE_ID_IDX), 0), 0, 1, 0);
            List<Object> primaryKey = indexes.get(0);
            List<List<Object>> parts = (List<List<Object>>) primaryKey.get(INDEX_FORMAT_IDX);
            for (int i = 0; i < parts.size(); i++) {
                List<Object> part = parts.get(i);
                int ordinal = ((Number) part.get(0)).intValue();
                String column = (String) fields.get(ordinal).get("name");
                rows.add(Arrays.asList(table, column, i + 1, "primary", primaryKey.get(NAME_IDX)));
            }
        }
        return new SQLNullResultSet((JDBCBridge.mock(
                Arrays.asList("TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME",
                        //nulls
                        "TABLE_CAT", "TABLE_SCHEM"
                ),
                rows)));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
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
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
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
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
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
    public int getJDBCMajorVersion() throws SQLException {
        return 2;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return rowOfNullsResultSet();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        return new SQLResultSet(JDBCBridge.EMPTY);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
