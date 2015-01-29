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

package org.apache.usergrid.persistence.collection.serialization.impl;


import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.EntitySet;
import org.apache.usergrid.persistence.collection.MvccEntity;
import org.apache.usergrid.persistence.collection.mvcc.MvccEntitySerializationStrategy;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamilyDefinition;
import org.apache.usergrid.persistence.core.guice.V1Impl;
import org.apache.usergrid.persistence.core.guice.V2Impl;
import org.apache.usergrid.persistence.core.migration.data.DataMigrationManager;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;


/**
 * Version 3 implementation of entity serialization. This will proxy writes and reads so that during
 * migration data goes to both sources and is read from the old source. After the ugprade completes,
 * it will be available from the new source
 */
@Singleton
public class MvccEntitySerializationStrategyProxyV1Impl extends MvccEntitySerializationStrategyProxy {

    public static final int MIGRATION_VERSION = 3;

    @Inject
    public MvccEntitySerializationStrategyProxyV1Impl(final DataMigrationManager dataMigrationManager,
                                                      final Keyspace keyspace,
                                                      @V1Impl final MvccEntitySerializationStrategy previous,
                                                      @V2Impl final MvccEntitySerializationStrategy current) {
        super(dataMigrationManager, keyspace, previous, current);
    }

    @Override
    public int getMigrationVersion() {
        return MIGRATION_VERSION;
    }
}
