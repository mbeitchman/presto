package com.facebook.presto.hive.metastore.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreEntityMapper.toGlueDatabaseInput;
import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreEntityMapper.toHivePartition;
import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreEntityMapper.toHiveTable;
import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreEntityMapper.toPrestoDatabaseEntity;

public class GlueHiveMetastore implements ExtendedHiveMetastore
{
    private AWSGlue glueDataCatalog;
    private int MAX_RESULTS = 100;

    public GlueHiveMetastore()
    {
        this.glueDataCatalog = AWSGlueAsyncClientBuilder.defaultClient();
    }

    public GlueHiveMetastore(AWSGlue glueDataCatalog)
    {
        this.glueDataCatalog = glueDataCatalog;
    }

    // Database Operations
    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest().withName(databaseName);
        GetDatabaseResult getDatabaseResult = glueDataCatalog.getDatabase(getDatabaseRequest);
        com.amazonaws.services.glue.model.Database db = getDatabaseResult.getDatabase();
        return Optional.of(toPrestoDatabaseEntity(db));
    }

    @Override
    public List<String> getAllDatabases()
    {
        List<String> databases = new ArrayList<>();
        Optional<String> token;

        do {
            GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest().withMaxResults(MAX_RESULTS);
            GetDatabasesResult result = glueDataCatalog.getDatabases(getDatabasesRequest);
            List<String> dbNames = result.getDatabaseList().stream().map(x -> x.getName()).collect(Collectors.toList());
            databases.addAll(dbNames);
            token = Optional.ofNullable(result.getNextToken());
        } while (token.isPresent());

        return databases;
    }

    @Override
    public void createDatabase(Database database)
    {
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest().withDatabaseInput(toGlueDatabaseInput(database));
        glueDataCatalog.createDatabase(createDatabaseRequest);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        Database db = getDatabase(databaseName).get();
        DatabaseInput databaseInput = new DatabaseInput().withName(newDatabaseName).withParameters(db.getParameters()).withDescription(db.getComment().get())
                .withLocationUri(db.getLocation().get());
        UpdateDatabaseRequest updateDatabaseRequest = new UpdateDatabaseRequest().withDatabaseInput(databaseInput).withName(newDatabaseName);
        glueDataCatalog.updateDatabase(updateDatabaseRequest);
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest().withName(databaseName);
        glueDataCatalog.deleteDatabase(deleteDatabaseRequest);
    }

    // Table Operations
    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        GetTableRequest getTableRequest = new GetTableRequest().withDatabaseName(databaseName).withName(tableName);
        GetTableResult getTableResult = glueDataCatalog.getTable(getTableRequest);
        Table t = toHiveTable(getTableResult.getTable());
        return Optional.of(t);
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        List<String> results = new ArrayList<>();
        Optional<String> token;

        do {
            GetTablesRequest tableRequest = new GetTablesRequest().withDatabaseName(databaseName).withMaxResults(MAX_RESULTS);
            GetTablesResult tablesResult = glueDataCatalog.getTables(tableRequest);
            List<String> tables = tablesResult.getTableList().stream().map(x -> x.getName()).collect(Collectors.toList());
            results.addAll(tables);
            token = Optional.ofNullable(tablesResult.getNextToken());
        } while (token.isPresent());

        return Optional.of(results);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        TableInput tableInput = new TableInput().withTableType(table.getTableType()).withName(table.getTableName()).withOwner(table.getOwner())
                .withParameters(table.getParameters()); //todo .withPartitionKeys(table.getPartitionColumns());
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableInput(tableInput).withDatabaseName(table.getDatabaseName());
        glueDataCatalog.createTable(createTableRequest);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withDatabaseName(databaseName)
                .withName(tableName);
        glueDataCatalog.deleteTable(deleteTableRequest);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
         dropTable(databaseName, tableName, false);
         createTable(newTable, null);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        Table table = getTable(databaseName, tableName).get();
        Table newTable = Table.builder().setOwner(table.getOwner()).setTableName(newTableName).setTableType(table.getTableType())
                .setDatabaseName(newDatabaseName).setDataColumns(table.getDataColumns()).setParameters(table.getParameters()).setPartitionColumns(table.getPartitionColumns())
                .setViewExpandedText(table.getViewExpandedText()).setViewOriginalText(table.getViewOriginalText()).build();
        dropTable(databaseName, tableName, false);
        createTable(newTable, null);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Table table = getTable(databaseName, tableName).get();
        Column column = new Column(columnName, columnType, Optional.of(columnComment));
        List<Column> tableColumns = table.getDataColumns();
        tableColumns.add(column);
        TableInput tableInput = new TableInput(); // todo add table input
        UpdateTableRequest updateTableRequest = new UpdateTableRequest().withTableInput(tableInput).withDatabaseName(databaseName);
        glueDataCatalog.updateTable(updateTableRequest);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        Table table = getTable(databaseName, tableName).get();
        List<Column> tableColumns = table.getDataColumns();
        List<Column> updatedColumns = new ArrayList<>();
        for(Column c : tableColumns) {
            if (c.getName().equals(oldColumnName)) {
                Column newColumn = new Column(newColumnName, c.getType(), c.getComment());
                updatedColumns.add(newColumn);
            } else {
                updatedColumns.add(c);
            }
        }
        TableInput tableInput = new TableInput(); // todo add table input
        UpdateTableRequest updateTableRequest = new UpdateTableRequest().withTableInput(tableInput).withDatabaseName(databaseName);
        glueDataCatalog.updateTable(updateTableRequest);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        Table table = getTable(databaseName, tableName).get();
        List<Column> tableColumns = table.getDataColumns();
        List<Column> updatedColumns = new ArrayList<>();
        for(Column c : tableColumns) {
            if (!c.getName().equals(columnName)) {
                updatedColumns.add(c);
            }
        }
        TableInput tableInput = new TableInput(); // todo add table input
        UpdateTableRequest updateTableRequest = new UpdateTableRequest().withTableInput(tableInput).withDatabaseName(databaseName);
        glueDataCatalog.updateTable(updateTableRequest);
    }

    // Partition Operations
    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        GetPartitionRequest getPartitionRequest = new GetPartitionRequest().withDatabaseName(databaseName).withTableName(tableName).withPartitionValues(partitionValues);
        GetPartitionResult getPartitionResult = glueDataCatalog.getPartition(getPartitionRequest);
        com.amazonaws.services.glue.model.Partition p = getPartitionResult.getPartition();
        return Optional.of(toHivePartition(p));
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        List<String> results = new ArrayList<>();
        Optional<String> token;

        do {
            GetPartitionsRequest partitionsRequest = new GetPartitionsRequest().withDatabaseName(databaseName).withTableName(tableName).withMaxResults(MAX_RESULTS);
            GetPartitionsResult partitionsResult = glueDataCatalog.getPartitions(partitionsRequest);
            List<String> partitions = partitionsResult.getPartitions().stream().map(x -> getPartitionName(x)).collect(Collectors.toList());
            results.addAll(partitions);
            token = Optional.ofNullable(partitionsResult.getNextToken());
        } while (token.isPresent());

        return Optional.of(results);
    }

    private String getPartitionName(com.amazonaws.services.glue.model.Partition partition)
    {
        // todo
        return "";
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        return null;
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        return null;
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<Partition> partitions)
    {

    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        DeletePartitionRequest deletePartitionRequest = new DeletePartitionRequest().withDatabaseName(databaseName)
                .withTableName(tableName).withPartitionValues(parts);
        glueDataCatalog.deletePartition(deletePartitionRequest);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
    {
        PartitionInput partitionInput = new PartitionInput(); // todo
        UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest().withPartitionInput(partitionInput).withDatabaseName(databaseName).withTableName(tableName);
        glueDataCatalog.updatePartition(updatePartitionRequest);
    }

    // Roles and Privileges operations
    @Override
    public Set<String> getRoles(String user)
    {
        throw new GlueMetastoreNotImplementedException(HIVE_METASTORE_ERROR, "getRoles() not supported.");
    }

    @Override
    public Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName)
    {
        throw new GlueMetastoreNotImplementedException(HIVE_METASTORE_ERROR, "getDatabasePrivileges() not supported.");
    }

    @Override
    public Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName)
    {
        throw new GlueMetastoreNotImplementedException(HIVE_METASTORE_ERROR, "getTablePrivileges() not supported.");
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new GlueMetastoreNotImplementedException(HIVE_METASTORE_ERROR, "grantTablePrivileges() not supported.");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new GlueMetastoreNotImplementedException(HIVE_METASTORE_ERROR, "revokeTablePrivileges() not supported.");
    }

    // Column stats
    @Override
    public Optional<Map<String, HiveColumnStatistics>> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        throw new GlueMetastoreNotImplementedException(HIVE_METASTORE_ERROR, "getTableColumnStatistics() not supported.");
    }

    @Override
    public Optional<Map<String, Map<String, HiveColumnStatistics>>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        throw new GlueMetastoreNotImplementedException(HIVE_METASTORE_ERROR, "getPartitionColumnStatistics() not supported.");
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        throw new GlueMetastoreNotImplementedException(HIVE_METASTORE_ERROR, "getAllViews() not supported.");
    }
}
