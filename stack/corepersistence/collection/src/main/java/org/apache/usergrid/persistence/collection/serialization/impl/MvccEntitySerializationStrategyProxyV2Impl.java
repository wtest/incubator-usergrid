package org.apache.usergrid.persistence.collection.serialization.impl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import org.apache.usergrid.persistence.collection.mvcc.MvccEntityMigrationStrategy;
import org.apache.usergrid.persistence.core.guice.*;
import org.apache.usergrid.persistence.core.migration.data.DataMigrationManager;


/**
 * Version 4 implementation of entity serialization. This will proxy writes and reads so that during
 * migration data goes to both sources and is read from the old source. After the ugprade completes,
 * it will be available from the new source
 */
@Singleton
public class MvccEntitySerializationStrategyProxyV2Impl extends MvccEntitySerializationStrategyProxy implements MvccEntityMigrationStrategy {


    @Inject
    public MvccEntitySerializationStrategyProxyV2Impl( final DataMigrationManager dataMigrationManager,
                                                     final Keyspace keyspace,
                                                     @V1ProxyImpl final MvccEntitySerializationStrategy previous,
                                                     @V3Impl final MvccEntitySerializationStrategy current) {
        super(dataMigrationManager,keyspace,previous,current);
    }

    @Override
    public int getMigrationVersion() {
        return V3Impl.MIGRATION_VERSION;
    }


    @Override
    public MigrationRelationship<MvccEntitySerializationStrategy> getMigration() {
        return new MigrationRelationship<>(this.previous,this.current);
    }



}
