package org.tarantool.jdbc;

import static org.tarantool.util.JdbcConstants.DatabaseMetadataTable;

import org.tarantool.SqlProtoUtils;
import org.tarantool.Version;

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
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.getProperties().getProperty("user");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Tarantool";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return connection.getServerVersion();
    }

    @Override
    public String getDriverName() throws SQLException {
        return SQLConstant.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
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
            List<List<Object>> spaces = (List<List<Object>>) connection.nativeSelect(
                _VSPACE, 0, Arrays.asList(), 0, SPACES_MAX, 0
            );
            List<List<Object>> rows = new ArrayList<List<Object>>();
            for (List<Object> space : spaces) {
                String tableName = (String) space.get(NAME_IDX);
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
            Collections.singletonList(Arrays.asList("TABLE"))
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
            List<List<Object>> spaces = (List<List<Object>>) connection.nativeSelect(
                _VSPACE, 0, Arrays.asList(), 0, SPACES_MAX, 0
            );
            List<List<Object>> rows = new ArrayList<>();
            for (List<Object> space : spaces) {
                String tableName = (String) space.get(NAME_IDX);
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
            List spaces = connection.nativeSelect(_VSPACE, 2, Collections.singletonList(table), 0, 1, 0);

            if (spaces == null || spaces.size() == 0) {
                return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
            }

            List space = ensureType(List.class, spaces.get(0));
            List fields = ensureType(List.class, space.get(FORMAT_IDX));
            int spaceId = ensureType(Number.class, space.get(SPACE_ID_IDX)).intValue();
            List indexes = connection.nativeSelect(_VINDEX, 0, Arrays.asList(spaceId, 0), 0, 1, 0);
            List primaryKey = ensureType(List.class, indexes.get(0));
            List parts = ensureType(List.class, primaryKey.get(INDEX_FORMAT_IDX));

            List<List<Object>> rows = new ArrayList<>();
            for (int i = 0; i < parts.size(); i++) {
                // For native spaces, the 'parts' is 'List of Lists'.
                // We only accept SQL spaces, for which the parts is 'List of Maps'.
                Map part = checkType(Map.class, parts.get(i));
                if (part == null) {
                    return asEmptyMetadataResultSet(DatabaseMetadataTable.PRIMARY_KEYS);
                }

                int ordinal = ensureType(Number.class, part.get("field")).intValue();
                Map field = ensureType(Map.class, fields.get(ordinal));
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
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY ||
            type == ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return supportsResultSetType(type) && concurrency == ResultSet.CONCUR_READ_ONLY;
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
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return asEmptyMetadataResultSet(DatabaseMetadataTable.SUPER_TYPES);
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
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
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
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
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

    private ResultSet asMetadataResultSet(List<String> columnNames, List<List<Object>> rows) throws SQLException {
        List<SqlProtoUtils.SQLMetaData> meta = columnNames.stream()
            .map(SqlProtoUtils.SQLMetaData::new)
            .collect(Collectors.toList());
        SQLResultHolder holder = SQLResultHolder.ofQuery(meta, rows);
        return createMetadataStatement().executeMetadata(holder);
    }

    private ResultSet asEmptyMetadataResultSet(List<String> columnNames) throws SQLException {
        return asMetadataResultSet(columnNames, Collections.emptyList());
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
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
