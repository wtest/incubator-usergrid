/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.usergrid.persistence.collection.serialization.impl;

import com.google.inject.Inject;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.EntitySet;
import org.apache.usergrid.persistence.collection.MvccEntity;
import org.apache.usergrid.persistence.collection.mvcc.MvccEntityMigrationStrategy;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamilyDefinition;
import org.apache.usergrid.persistence.core.migration.data.DataMigration;
import org.apache.usergrid.persistence.core.migration.data.DataMigrationException;
import org.apache.usergrid.persistence.core.migration.data.DataMigrationManager;
import org.apache.usergrid.persistence.core.migration.schema.MigrationStrategy;
import org.apache.usergrid.persistence.core.scope.ApplicationEntityGroup;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.model.entity.Id;
import org.apache.usergrid.persistence.model.util.UUIDGenerator;
import rx.Observable;
import rx.functions.Func1;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Version 4 implementation of entity serialization. This will proxy writes and reads so that during
 * migration data goes to both sources and is read from the old source. After the ugprade completes,
 * it will be available from the new source
 */
public  abstract class MvccEntitySerializationStrategyProxy implements MvccEntitySerializationStrategy, MvccEntityMigrationStrategy {


    private final DataMigrationManager dataMigrationManager;
    protected final Keyspace keyspace;
    protected final MvccEntitySerializationStrategy previous;
    protected final MvccEntitySerializationStrategy current;


    @Inject
    public MvccEntitySerializationStrategyProxy( final DataMigrationManager dataMigrationManager,
                                                       final Keyspace keyspace,
                                                      final MvccEntitySerializationStrategy previous,
                                                       final MvccEntitySerializationStrategy current) {
        this.dataMigrationManager = dataMigrationManager;
        this.keyspace = keyspace;
        this.previous = previous;
        this.current = current;
    }


    @Override
    public MutationBatch write( final CollectionScope context, final MvccEntity entity ) {
        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.write( context, entity ) );
            aggregateBatch.mergeShallow( current.write( context, entity ) );

            return aggregateBatch;
        }

        return current.write( context, entity );
    }


    @Override
    public EntitySet load( final CollectionScope scope, final Collection<Id> entityIds, final UUID maxVersion ) {
        if ( isOldVersion() ) {
            return previous.load( scope, entityIds, maxVersion );
        }

        return current.load( scope, entityIds, maxVersion );
    }


    @Override
    public Iterator<MvccEntity> loadDescendingHistory( final CollectionScope context, final Id entityId,
                                                       final UUID version, final int fetchSize ) {
        if ( isOldVersion() ) {
            return previous.loadDescendingHistory( context, entityId, version, fetchSize );
        }

        return current.loadDescendingHistory( context, entityId, version, fetchSize );
    }


    @Override
    public Iterator<MvccEntity> loadAscendingHistory( final CollectionScope context, final Id entityId,
                                                      final UUID version, final int fetchSize ) {
        if ( isOldVersion() ) {
            return previous.loadAscendingHistory( context, entityId, version, fetchSize );
        }

        return current.loadAscendingHistory( context, entityId, version, fetchSize );
    }


    @Override
    public MutationBatch mark( final CollectionScope context, final Id entityId, final UUID version ) {
        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.mark( context, entityId, version ) );
            aggregateBatch.mergeShallow( current.mark( context, entityId, version ) );

            return aggregateBatch;
        }

        return current.mark( context, entityId, version );
    }


    @Override
    public MutationBatch delete( final CollectionScope context, final Id entityId, final UUID version ) {
        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.delete( context, entityId, version ) );
            aggregateBatch.mergeShallow( current.delete( context, entityId, version ) );

            return aggregateBatch;
        }

        return current.delete( context, entityId, version );
    }

    /**
     * Return true if we're on an old version
     */
    private boolean isOldVersion() {
        return dataMigrationManager.getCurrentVersion() < getMigrationVersion();
    }


    @Override
    public Collection<MultiTennantColumnFamilyDefinition> getColumnFamilies() {
        return Collections.emptyList();
    }

    @Override
    public Observable<Long> executeMigration(final Observable<MvccEntity> entityObservable,final ApplicationEntityGroup applicationEntityGroup, final DataMigration.ProgressObserver observer, final Func1<Id,? extends ApplicationScope> getCollectionScopeFromEntityId) {
        final AtomicLong atomicLong = new AtomicLong();
        final MutationBatch totalBatch = keyspace.prepareMutationBatch();

        final List<Id> entityIds = applicationEntityGroup.entityIds;

        final UUID now = UUIDGenerator.newTimeUUID();

        //go through each entity in the system, and load it's entire
        // history
        return Observable.from(entityIds)

            .map(new Func1<Id, Id>() {
                @Override
                public Id call(Id entityId) {

                    ApplicationScope applicationScope = getCollectionScopeFromEntityId.call(entityId);

                    if (!(applicationScope instanceof CollectionScope)) {
                        throw new IllegalArgumentException("getCollectionScopeFromEntityId must return a collection scope");
                    }
                    CollectionScope currentScope = (CollectionScope) applicationScope;
                    MigrationStrategy.MigrationRelationship<MvccEntitySerializationStrategy> migration = getMigration();
                    //for each element in the history in the previous version,
                    // copy it to the CF in v2
                    Iterator<MvccEntity> allVersions = migration.from()
                        .loadDescendingHistory(currentScope, entityId, now,
                            1000);

                    while (allVersions.hasNext()) {
                        final MvccEntity version = allVersions.next();

                        final MutationBatch versionBatch =
                            migration.to().write(currentScope, version);

                        totalBatch.mergeShallow(versionBatch);

                        if (atomicLong.incrementAndGet() % 50 == 0) {
                            executeBatch(totalBatch, observer, atomicLong);
                        }
                    }
                    executeBatch(totalBatch, observer, atomicLong);
                    return entityId;
                }
            })
            .map(new Func1<Id, Long>() {
                @Override
                public Long call(Id id) {
                    executeBatch(totalBatch, observer, atomicLong);
                    return atomicLong.get();
                }
            });
    }

    protected void executeBatch( final MutationBatch batch, final DataMigration.ProgressObserver po, final AtomicLong count ) {
        try {
            batch.execute();

            po.update( getMigrationVersion(), "Finished copying " + count + " entities to the new format" );
        }
        catch ( ConnectionException e ) {
            po.failed( getMigrationVersion(), "Failed to execute mutation in cassandra" );
            throw new DataMigrationException( "Unable to migrate batches ", e );
        }
    }

}

