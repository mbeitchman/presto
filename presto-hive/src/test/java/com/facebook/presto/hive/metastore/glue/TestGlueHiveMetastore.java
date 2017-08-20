package com.facebook.presto.hive.metastore.glue;

import org.testng.annotations.Test;

public class TestGlueHiveMetastore
{
    //AWSGlue glue = mock();
    GlueHiveMetastore glueHiveMetastore = new GlueHiveMetastore();

    @Test
    public void testGetTable()
    {
        glueHiveMetastore.getTable("", "");
    }
}
