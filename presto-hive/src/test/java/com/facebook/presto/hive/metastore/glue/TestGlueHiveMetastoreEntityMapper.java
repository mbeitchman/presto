package com.facebook.presto.hive.metastore.glue;

import com.amazonaws.services.glue.model.DatabaseInput;
import com.facebook.presto.hive.metastore.Database;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastoreEntityMapper.toGlueDatabaseInput;
import static org.testng.Assert.assertEquals;

public class TestGlueHiveMetastoreEntityMapper
{
    @Test
    public void testToGlueDatabaseInput() {
        Database db = Database.builder().setDatabaseName("foo").build();
        DatabaseInput databaseInput = toGlueDatabaseInput(db);
        assertEquals("foo", databaseInput.getName());
    }
}
