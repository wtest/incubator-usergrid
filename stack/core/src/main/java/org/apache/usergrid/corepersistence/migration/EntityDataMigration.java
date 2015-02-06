/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.usergrid.corepersistence.migration;


import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.usergrid.corepersistence.ManagerCache;
import org.apache.usergrid.corepersistence.rx.AllEntitiesInSystemObservable;
import org.apache.usergrid.corepersistence.util.CpNamingUtils;
import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.mvcc.MvccEntityMigrationStrategy;
import org.apache.usergrid.persistence.core.migration.data.DataMigration;
import org.apache.usergrid.persistence.core.scope.ApplicationEntityGroup;

import com.google.inject.Inject;
import org.apache.usergrid.persistence.model.entity.Id;
import rx.functions.Action1;
import rx.functions.Func1;


/**
 * Migration for migrating graph edges to the new Shards
 */
public class EntityDataMigration implements DataMigration {


    private final ManagerCache managerCache;
    private final MvccEntityMigrationStrategy migrationStrategy;

    @Inject
    public EntityDataMigration(  final MvccEntityMigrationStrategy migrationStrategy,
                                final ManagerCache managerCache ) {
        this.migrationStrategy = migrationStrategy;
        this.managerCache = managerCache;
    }

    @Override
    public void migrate( final ProgressObserver observer ) throws Throwable {
        AllEntitiesInSystemObservable
            .getAllEntitiesInSystem(managerCache, 1000)
            .doOnNext(new Action1<ApplicationEntityGroup>() {
                @Override
                public void call( final ApplicationEntityGroup applicationEntityGroup ) {
                    migrationStrategy.executeMigration( null,applicationEntityGroup, observer, CpNamingUtils.getCollectionScopeByEntityIdFunc1(applicationEntityGroup.applicationScope));
                }
            })
            .toBlocking().last();
    }

    @Override
    public int getVersion() {
        return migrationStrategy.getMigrationVersion();
    }
}
