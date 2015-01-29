package org.apache.usergrid.persistence.collection.serialization.impl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.EntitySet;
import org.apache.usergrid.persistence.collection.MvccEntity;
import org.apache.usergrid.persistence.collection.mvcc.MvccEntitySerializationStrategy;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamilyDefinition;
import org.apache.usergrid.persistence.core.guice.*;
import org.apache.usergrid.persistence.core.migration.data.DataMigrationManager;
import org.apache.usergrid.persistence.model.entity.Id;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

/**
 * Version 4 implementation of entity serialization. This will proxy writes and reads so that during
 * migration data goes to both sources and is read from the old source. After the ugprade completes,
 * it will be available from the new source
 */
@Singleton
public class MvccEntitySerializationStrategyProxyV2Impl extends MvccEntitySerializationStrategyProxy {

    public static final int MIGRATION_VERSION = 4;

    @Inject
    public MvccEntitySerializationStrategyProxyV2Impl( final DataMigrationManager dataMigrationManager,
                                                     final Keyspace keyspace,
                                                     @V1ProxyImpl final MvccEntitySerializationStrategy previous,
                                                     @V3Impl final MvccEntitySerializationStrategy current) {
        super(dataMigrationManager,keyspace,previous,current);
    }

    @Override
    public int getMigrationVersion() {
        return MIGRATION_VERSION;
    }

}
