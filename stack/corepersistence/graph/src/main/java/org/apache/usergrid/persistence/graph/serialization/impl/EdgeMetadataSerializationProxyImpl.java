/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.usergrid.persistence.graph.serialization.impl;


import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamilyDefinition;
import org.apache.usergrid.persistence.core.guice.V1Impl;
import org.apache.usergrid.persistence.core.guice.V2Impl;
import org.apache.usergrid.persistence.core.migration.data.DataMigration;
import org.apache.usergrid.persistence.core.migration.data.DataMigrationManager;
import org.apache.usergrid.persistence.core.scope.ApplicationEntityGroup;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.*;
import org.apache.usergrid.persistence.graph.impl.SimpleSearchByEdgeType;
import org.apache.usergrid.persistence.graph.impl.SimpleSearchEdgeType;
import org.apache.usergrid.persistence.graph.serialization.EdgeMetadataSerialization;
import org.apache.usergrid.persistence.graph.serialization.EdgeMigrationStrategy;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;


@Singleton
public class EdgeMetadataSerializationProxyImpl implements EdgeMetadataSerialization, EdgeMigrationStrategy {

    private static final Logger logger = LoggerFactory.getLogger(EdgeMetadataSerializationProxyImpl.class);

    private final DataMigrationManager dataMigrationManager;
    private final Keyspace keyspace;
    private final EdgeMetadataSerialization previous;
    private final EdgeMetadataSerialization current;


    /**
     * Handles routing data to the right implementation, based on the current system migration version
     */
    @Inject
    public EdgeMetadataSerializationProxyImpl(final DataMigrationManager dataMigrationManager, final Keyspace keyspace,
                                              @V1Impl final EdgeMetadataSerialization previous,
                                              @V2Impl final EdgeMetadataSerialization current) {
        this.dataMigrationManager = dataMigrationManager;
        this.keyspace = keyspace;
        this.previous = previous;
        this.current = current;
    }


    @Override
    public MutationBatch writeEdge( final ApplicationScope scope, final Edge edge ) {


        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.writeEdge( scope, edge ) );
            aggregateBatch.mergeShallow( current.writeEdge( scope, edge ) );

            return aggregateBatch;
        }

        return current.writeEdge( scope, edge );
    }


    @Override
    public MutationBatch removeEdgeTypeFromSource( final ApplicationScope scope, final Edge edge ) {

        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.removeEdgeTypeFromSource( scope, edge ) );
            aggregateBatch.mergeShallow( current.removeEdgeTypeFromSource( scope, edge ) );

            return aggregateBatch;
        }

        return current.removeEdgeTypeFromSource( scope, edge );
    }


    @Override
    public MutationBatch removeEdgeTypeFromSource( final ApplicationScope scope, final Id sourceNode, final String type,
                                                   final long timestamp ) {


        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.removeEdgeTypeFromSource( scope, sourceNode, type, timestamp ) );
            aggregateBatch.mergeShallow( current.removeEdgeTypeFromSource( scope, sourceNode, type, timestamp ) );

            return aggregateBatch;
        }

        return current.removeEdgeTypeFromSource( scope, sourceNode, type, timestamp );
    }


    @Override
    public MutationBatch removeIdTypeFromSource( final ApplicationScope scope, final Edge edge ) {

        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.removeIdTypeFromSource( scope, edge ) );
            aggregateBatch.mergeShallow( current.removeIdTypeFromSource( scope, edge ) );

            return aggregateBatch;
        }

        return current.removeIdTypeFromSource( scope, edge );
    }


    @Override
    public MutationBatch removeIdTypeFromSource( final ApplicationScope scope, final Id sourceNode, final String type,
                                                 final String idType, final long timestamp ) {

        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch
                    .mergeShallow( previous.removeIdTypeFromSource( scope, sourceNode, type, idType, timestamp ) );
            aggregateBatch.mergeShallow( current.removeIdTypeFromSource( scope, sourceNode, type, idType, timestamp ) );

            return aggregateBatch;
        }

        return current.removeIdTypeFromSource( scope, sourceNode, type, idType, timestamp );
    }


    @Override
    public MutationBatch removeEdgeTypeToTarget( final ApplicationScope scope, final Edge edge ) {


        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.removeEdgeTypeToTarget( scope, edge ) );
            aggregateBatch.mergeShallow( current.removeEdgeTypeToTarget( scope, edge ) );

            return aggregateBatch;
        }

        return current.removeEdgeTypeToTarget( scope, edge );
    }


    @Override
    public MutationBatch removeEdgeTypeToTarget( final ApplicationScope scope, final Id targetNode, final String type,
                                                 final long timestamp ) {

        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.removeEdgeTypeToTarget( scope, targetNode, type, timestamp ) );
            aggregateBatch.mergeShallow( current.removeEdgeTypeToTarget( scope, targetNode, type, timestamp ) );

            return aggregateBatch;
        }

        return current.removeEdgeTypeToTarget( scope, targetNode, type, timestamp );
    }


    @Override
    public MutationBatch removeIdTypeToTarget( final ApplicationScope scope, final Edge edge ) {

        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.removeIdTypeToTarget( scope, edge ) );
            aggregateBatch.mergeShallow( current.removeIdTypeToTarget( scope, edge ) );

            return aggregateBatch;
        }

        return current.removeIdTypeToTarget( scope, edge );
    }


    @Override
    public MutationBatch removeIdTypeToTarget( final ApplicationScope scope, final Id targetNode, final String type,
                                               final String idType, final long timestamp ) {


        if ( isOldVersion() ) {
            final MutationBatch aggregateBatch = keyspace.prepareMutationBatch();

            aggregateBatch.mergeShallow( previous.removeIdTypeToTarget( scope, targetNode, type, idType, timestamp ) );
            aggregateBatch.mergeShallow( current.removeIdTypeToTarget( scope, targetNode, type, idType, timestamp ) );

            return aggregateBatch;
        }

        return current.removeIdTypeToTarget( scope, targetNode, type, idType, timestamp );
    }


    @Override
    public Iterator<String> getEdgeTypesFromSource( final ApplicationScope scope, final SearchEdgeType search ) {
        if ( isOldVersion() ) {
            return previous.getEdgeTypesFromSource( scope, search );
        }

        return current.getEdgeTypesFromSource( scope, search );
    }


    @Override
    public Iterator<String> getIdTypesFromSource( final ApplicationScope scope, final SearchIdType search ) {
        if ( isOldVersion() ) {
            return previous.getIdTypesFromSource( scope, search );
        }

        return current.getIdTypesFromSource( scope, search );
    }


    @Override
    public Iterator<String> getEdgeTypesToTarget( final ApplicationScope scope, final SearchEdgeType search ) {
        if ( isOldVersion() ) {
            return previous.getEdgeTypesToTarget( scope, search );
        }

        return current.getEdgeTypesToTarget( scope, search );
    }


    @Override
    public Iterator<String> getIdTypesToTarget( final ApplicationScope scope, final SearchIdType search ) {
        if ( isOldVersion() ) {
            return previous.getIdTypesToTarget( scope, search );
        }

        return current.getIdTypesToTarget( scope, search );
    }


    @Override
    public Collection<MultiTennantColumnFamilyDefinition> getColumnFamilies() {
        return Collections.EMPTY_LIST;
    }


    /**
     * Return true if we're on an old version
     */
    private boolean isOldVersion() {
        return dataMigrationManager.getCurrentVersion() < MIGRATION_VERSION;
    }

    @Override
    public MigrationRelationship<EdgeMetadataSerialization> getMigration() {
        return new MigrationRelationship<>(previous,current);
    }

    @Override
    public int getMigrationVersion() {
        return EdgeMigrationStrategy.MIGRATION_VERSION;
    }

    @Override
    public Observable<Long> executeMigration(final Observable<Edge> edgesFromSource ,final ApplicationEntityGroup applicationEntityGroup,
                                             final DataMigration.ProgressObserver observer,
                                             Func1<Id, ? extends ApplicationScope> getScopeFromEntityId) {
        final AtomicLong counter = new AtomicLong();
        rx.Observable o =
            Observable
                .from(applicationEntityGroup.entityIds)

            .flatMap(new Func1<Id, Observable<List<Edge>>>() {
                //for each id in the group, get it's edges
                @Override
                public Observable<List<Edge>> call(final Id id) {
                    logger.info("Migrating edges from node {} in scope {}", id,
                        applicationEntityGroup.applicationScope);


                    //get each edge from this node as a source
                    return edgesFromSource

                        //for each edge, re-index it in v2  every 1000 edges or less
                        .buffer(1000)
                        .doOnNext(new Action1<List<Edge>>() {
                            @Override
                            public void call(List<Edge> edges) {
                                final MutationBatch batch =
                                    keyspace.prepareMutationBatch();

                                for (Edge edge : edges) {
                                    logger.info("Migrating meta for edge {}", edge);
                                    final MutationBatch edgeBatch = getMigration().to()
                                        .writeEdge(
                                            applicationEntityGroup
                                                .applicationScope,
                                            edge);
                                    batch.mergeShallow(edgeBatch);
                                }

                                try {
                                    batch.execute();
                                } catch (ConnectionException e) {
                                    throw new RuntimeException(
                                        "Unable to perform migration", e);
                                }

                                //update the observer so the admin can see it
                                final long newCount =
                                    counter.addAndGet(edges.size());

                                observer.update(getMigrationVersion(), String.format(
                                    "Currently running.  Rewritten %d edge types",
                                    newCount));
                            }
                        });
                }


            })
            .map(new Func1<List<Edge>, Long>() {
                @Override
                public Long call(List<Edge> edges) {
                    return counter.get();
                }
            });
        return o;
    }


}
