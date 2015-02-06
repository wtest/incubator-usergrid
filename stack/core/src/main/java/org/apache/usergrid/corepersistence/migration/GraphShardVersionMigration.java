/*
 *
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
 *
 */

package org.apache.usergrid.corepersistence.migration;


import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.usergrid.corepersistence.util.CpNamingUtils;
import org.apache.usergrid.persistence.collection.impl.CollectionScopeImpl;
import org.apache.usergrid.persistence.core.scope.ApplicationEntityGroup;
import org.apache.usergrid.persistence.core.scope.ApplicationScope;
import org.apache.usergrid.persistence.graph.serialization.EdgeMigrationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.corepersistence.ManagerCache;
import org.apache.usergrid.corepersistence.rx.AllEntitiesInSystemObservable;
import org.apache.usergrid.corepersistence.rx.EdgesFromSourceObservable;
import org.apache.usergrid.persistence.core.migration.data.DataMigration;
import org.apache.usergrid.persistence.graph.Edge;
import org.apache.usergrid.persistence.graph.GraphManager;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.inject.Inject;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;


/**
 * Migration for migrating graph edges to the new Shards
 */
public class GraphShardVersionMigration implements DataMigration {


    private final ManagerCache managerCache;
    private final EdgeMigrationStrategy migrationStrategy;


    @Inject
    public GraphShardVersionMigration( final EdgeMigrationStrategy migrationStrategy,
                                       final ManagerCache managerCache ) {
        this.migrationStrategy = migrationStrategy;
        this.managerCache = managerCache;
    }


    @Override
    public void migrate( final ProgressObserver observer ) throws Throwable {

        AllEntitiesInSystemObservable.getAllEntitiesInSystem( managerCache, 1000 )
            .flatMap(
                new Func1<ApplicationEntityGroup, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(
                        final ApplicationEntityGroup applicationEntityGroup) {
                        final GraphManager gm = managerCache.getGraphManager(applicationEntityGroup.applicationScope);
                        Observable<Edge> edgesFromSource = EdgesFromSourceObservable.edgesFromSource(gm, applicationEntityGroup.applicationScope.getApplication());
                        //emit a stream of all ids from this group
                        return migrationStrategy.executeMigration(edgesFromSource,applicationEntityGroup, observer, CpNamingUtils.getCollectionScopeByEntityIdFunc1(applicationEntityGroup.applicationScope));
                    }
                } )
            .toBlocking().lastOrDefault( null );
        ;
    }


    @Override
    public int getVersion() {
        return migrationStrategy.getMigrationVersion();
    }
}
