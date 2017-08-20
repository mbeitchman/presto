package com.facebook.presto.hive.metastore.glue;

import com.amazonaws.services.glue.model.DatabaseInput;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class GlueHiveMetastoreEntityMapper
{
    public static Database toPrestoDatabaseEntity(com.amazonaws.services.glue.model.Database database) {
        Optional<String> comment = Optional.ofNullable(database.getDescription());
        String name = database.getName();
        Optional<String> location = Optional.of(database.getLocationUri());
        Map<String, String> parameters = database.getParameters();
        database.addParametersEntry("CreateTime", database.getCreateTime().toString());

        return Database.builder().setComment(comment).setLocation(location).setDatabaseName(name)
                .setLocation(location).setParameters(parameters).build();
    }

    public static DatabaseInput toGlueDatabaseInput(Database database) {
        return new DatabaseInput().withName(database.getDatabaseName())
                .withLocationUri(database.getLocation().get()).withDescription(database.getComment().map(c -> c).orElse(""))
                .withParameters(database.getParameters());
    }

    public static Table toHiveTable(com.amazonaws.services.glue.model.Table table) {
        // todo params
        return Table.builder().setTableName(table.getName()).setTableType(table.getTableType())
                .setDatabaseName(table.getDatabaseName()).setOwner(table.getOwner()).build();
    }

    public static Partition toHivePartition(com.amazonaws.services.glue.model.Partition partition) {
        List<Column> columns = new ArrayList();//partition.getStorageDescriptor().getColumns().stream().map( c -> getHiveColumn(c)).collect(Collectors.toList());

        return Partition.builder().setColumns(columns).setDatabaseName(partition.getDatabaseName()).setTableName(partition.getTableName())
                .setParameters(partition.getParameters()).setValues(partition.getValues()).build();
    }

    private static Column getHiveColumn(com.amazonaws.services.glue.model.Column c)
    {
        Column col = new Column(c.getName(), HiveType.HIVE_BYTE, Optional.of(c.getComment()));
        return col;
    }
}
