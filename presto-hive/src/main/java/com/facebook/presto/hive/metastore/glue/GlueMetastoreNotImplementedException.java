package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;

public class GlueMetastoreNotImplementedException
        extends PrestoException
{

    public GlueMetastoreNotImplementedException(ErrorCodeSupplier errorCode, String message)
    {
        super(errorCode, message);
    }
}
