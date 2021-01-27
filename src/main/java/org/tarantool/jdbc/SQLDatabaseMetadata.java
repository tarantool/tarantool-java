package org.tarantool.jdbc;

import static org.tarantool.util.JdbcConstants.DatabaseMetadataTable;

import org.tarantool.SqlProtoUtils;
import org.tarantool.Version;
import org.tarantool.jdbc.type.TarantoolSqlType;
import org.tarantool.util.TupleTwo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SQLDatabaseMetadata implements DatabaseMetaData {

    protected static final int _VSPACE = 281;
    protected static final int _VINDEX = 289;
    protected static final int SPACES_MAX = 65535;
    public static final int FORMAT_IDX = 6;
    public static final int NAME_IDX = 2;
    public static final int INDEX_FORMAT_IDX = 5;
    public static final int SPACE_ID_IDX = 0;
    protected final SQLConnection connection;

    public SQLDatabaseMetadata(SQLConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.STORED_PROCEDURES);
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public String getURL() {
        return connection.getUrl();
    }

    @Override
    public String getUserName() {
        return connection.getProperties().getProperty("user");
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public String getDatabaseProductName() {
        return "Tarantool";
    }

    @Override
    public String getDatabaseProductVersion() {
        return connection.getServerVersion();
    }

    @Override
    public String getDriverName() {
        return SQLConstant.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() {
        return Version.version;
    }

    @Override
    public int getDriverMajorVersion() {
        return Version.majorVersion;
    }

    @Override
    public int getDriverMinorVersion() {
        return Version.minorVersion;
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() {
        return " ";
    }

    @Override
    public String getSQLKeywords() {
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return "";
    }

    @Override
    public String getStringFunctions() {
        return "";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return null;
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsUnion() {
        return true;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog,
                                         String schemaPattern,
                                         String procedureNamePattern,
                                         String columnNamePattern)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.STORED_PROCEDURE_COLUMNS);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
        throws SQLException {
        try {
            if (types != null && !Arrays.asList(types).contains("TABLE")) {
                connection.checkNotClosed();
                return asEmptyMetadataResultSet(DatabaseMetadataTable.TABLES);
            }
            String[] parts = tableNamePattern == null ? new String[] { "" } : tableNamePattern.split("%");
            @SuppressWarnings("unchecked")
            List<List<Object>> spaces = (List<List<Object>>) connection.nativeSelect(
                _VSPACE, 0, Collections.emptyList(), 0, SPACES_MAX, 0
            );
            List<List<Object>> rows = new ArrayList<>();
            for (List<Object> space : spaces) {
                String tableName = (String) space.get(NAME_IDX);
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> format = (List<Map<String, Object>>) space.get(FORMAT_IDX);
                /*
                 * Skip Tarantool system spaces (started with underscore).
                 * Skip spaces that don't have format. Tarantool/SQL does not support such spaces.
                 */
                if (!tableName.startsWith("_") && format.size() > 0 && like(tableName, parts)) {
                    rows.add(Arrays.asList(
                        null, null,
                        tableName,
                        "TABLE",
                        null, null,
                        null, null,
                        null, null)
                    );
                }
            }
            return asMetadataResultSet(DatabaseMetadataTable.TABLES, rows);
        } catch (Exception e) {
            throw new SQLException(
                "Failed to retrieve table(s) description: " +
                    "tableNamePattern=\"" + tableNamePattern + "\".", e);
        }
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
    public ResultSet getTableTypes() throws SQLException {
        return asMetadataResultSet(
            DatabaseMetadataTable.TABLE_TYPES,
            Collections.singletonList(Collections.singletonList("TABLE"))
        );
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.SCHEMAS);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.SCHEMAS);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.CATALOGS);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.BEST_ROW_IDENTIFIER);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
        throws SQLException {
        try {
            String[] tableParts = tableNamePattern == null ? new String[] { "" } : tableNamePattern.split("%");
            String[] colParts = columnNamePattern == null ? new String[] { "" } : columnNamePattern.split("%");
            @SuppressWarnings("unchecked")
            List<List<Object>> spaces = (List<List<Object>>) connection.nativeSelect(
                _VSPACE, 0, Collections.emptyList(), 0, SPACES_MAX, 0
            );
            List<List<Object>> rows = new ArrayList<>();
            for (List<Object> space : spaces) {
                String tableName = (String) space.get(NAME_IDX);
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> format = (List<Map<String, Object>>) space.get(FORMAT_IDX);
                /*
                 * Skip Tarantool system spaces (started with underscore).
                 * Skip spaces that don't have format. Tarantool/SQL does not support such spaces.
                 */
                if (!tableName.startsWith("_") && format.size() > 0 && like(tableName, tableParts)) {
                    for (int columnIdx = 1; columnIdx <= format.size(); columnIdx++) {
                        Map<String, Object> f = format.get(columnIdx - 1);
                        String columnName = (String) f.get("name");
                        String typeName = (String) f.get("type");
                        if (like(columnName, colParts)) {
                            rows.add(Arrays.asList(
                                null, // table catalog
                                null, // table schema
                                tableName,
                                columnName,
                                Types.OTHER, // data type
                                typeName,
                                null, // column size
                                null, // buffer length
                                null, // decimal digits
                                10, // num prec radix
                                columnNullableUnknown,
                                null, // remarks
                                null, // column def
                                null, // sql data type
                                null, // sql datatype sub
                                null, // char octet length
                                columnIdx, // ordinal position
                                "", // is nullable
                                null, // scope catalog
                                null, // scope schema
                                null, // scope table
                                Types.OTHER, // source data type
                                "NO", // is autoincrement
                                "NO") // is generated column
                            );
                        }
                    }
                }
            }

            return asMetadataResultSet(DatabaseMetadataTable.COLUMNS, rows);
        } catch (Exception e) {
            throw new SQLException(
                "Error processing table column metadata: " +
                    "tableNamePattern=\"" + tableNamePattern + "\"; " +
                    "columnNamePattern=\"" + columnNamePattern + "\".", e);
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.COLUMN_PRIVILEGES);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.TABLE_PRIVILEGES);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.VERSION_COLUMNS);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.FOREIGN_KEYS);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        if (table == null || table.isEmpty()) {
            connection.checkNotClosed();
            return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
        }

        try {
            List<?> spaces = connection.nativeSelect(_VSPACE, 2, Collections.singletonList(table), 0, 1, 0);

            if (spaces == null || spaces.size() == 0) {
                return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
            }

            List<?> space = ensureType(List.class, spaces.get(0));
            List<?> fields = ensureType(List.class, space.get(FORMAT_IDX));
            int spaceId = ensureType(Number.class, space.get(SPACE_ID_IDX)).intValue();
            List<?> indexes = connection.nativeSelect(_VINDEX, 0, Arrays.asList(spaceId, 0), 0, 1, 0);
            List<?> primaryKey = ensureType(List.class, indexes.get(0));
            List<?> parts = ensureType(List.class, primaryKey.get(INDEX_FORMAT_IDX));

            List<List<Object>> rows = new ArrayList<>();
            for (int i = 0; i < parts.size(); i++) {
                // For native spaces, the 'parts' is 'List of Lists'.
                // We only accept SQL spaces, for which the parts is 'List of Maps'.
                Map<?, ?> part = checkType(Map.class, parts.get(i));
                if (part == null) {
                    return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
                }

                int ordinal = ensureType(Number.class, part.get("field")).intValue();
                Map<?, ?> field = ensureType(Map.class, fields.get(ordinal));
                // The 'name' field is optional in the format structure. But it is present for SQL space.
                String column = ensureType(String.class, field.get("name"));
                rows.add(Arrays.asList(null, null, table, column, i + 1, primaryKey.get(NAME_IDX)));
            }
            // Sort results by column name.
            rows.sort((left, right) -> {
                String col0 = (String) left.get(3);
                String col1 = (String) right.get(3);
                return col0.compareTo(col1);
            });
            return asMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS, rows);
        } catch (Exception e) {
            throw new SQLException("Error processing metadata for table \"" + table + "\".", e);
        }
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.FOREIGN_KEYS);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog,
                                       String parentSchema,
                                       String parentTable,
                                       String foreignCatalog,
                                       String foreignSchema,
                                       String foreignTable)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.FOREIGN_KEYS);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.TYPE_INFO);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.INDEX_INFO);
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.UDTS);
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY ||
            type == ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return supportsResultSetType(type) && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return true;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.SUPER_TYPES);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.SUPER_TABLES);
    }

    @Override
    public ResultSet getAttributes(String catalog,
                                   String schemaPattern,
                                   String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.ATTRIBUTES);
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.CLIENT_INFO_PROPERTIES);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Support of {@link ResultSet#CLOSE_CURSORS_AT_COMMIT} is not
     * available now because it requires cursor transaction support.
     */
    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 2;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 1;
    }

    @Override
    public int getSQLStateType() {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.FUNCTIONS);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog,
                                        String schemaPattern,
                                        String functionNamePattern,
                                        String columnNamePattern)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.FUNCTION_COLUMNS);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog,
                                      String schemaPattern,
                                      String tableNamePattern,
                                      String columnNamePattern)
        throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.PSEUDO_COLUMNS);
    }

    private ResultSet asMetadataResultSet(List<TupleTwo<String, TarantoolSqlType>> meta, List<List<Object>> rows)
        throws SQLException {
        List<SqlProtoUtils.SQLMetaData> sqlMeta = meta.stream()
            .map(tuple -> new SqlProtoUtils.SQLMetaData(tuple.getFirst(), tuple.getSecond()))
            .collect(Collectors.toList());
        SQLResultHolder holder = SQLResultHolder.ofQuery(sqlMeta, rows);
        return createMetadataStatement().executeMetadata(holder);
    }

    private ResultSet asEmptyMetadataResultSet(List<TupleTwo<String, TarantoolSqlType>> meta) throws SQLException {
        return asMetadataResultSet(meta, Collections.emptyList());
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return true;
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (isWrapperFor(type)) {
            return type.cast(this);
        }
        throw new SQLNonTransientException("SQLDatabaseMetadata does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return type.isAssignableFrom(this.getClass());
    }

    private TarantoolStatement createMetadataStatement() throws SQLException {
        return connection.createStatement().unwrap(TarantoolStatement.class);
    }

    private static <T> T ensureType(Class<T> cls, Object v) throws Exception {
        if (v == null || !cls.isAssignableFrom(v.getClass())) {
            throw new Exception(String.format("Wrong value type '%s', expected '%s'.",
                v == null ? "null" : v.getClass().getName(), cls.getName()));
        }
        return cls.cast(v);
    }

    private static <T> T checkType(Class<T> cls, Object v) {
        return (v != null && cls.isAssignableFrom(v.getClass())) ? cls.cast(v) : null;
    }

}
