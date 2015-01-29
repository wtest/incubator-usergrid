package org.apache.usergrid.persistence.collection.serialization.impl;

import com.google.inject.Inject;
import com.netflix.astyanax.Keyspace;
import org.apache.usergrid.persistence.collection.guice.TestCollectionModule;
import org.apache.usergrid.persistence.collection.mvcc.MvccEntitySerializationStrategy;
import org.apache.usergrid.persistence.collection.serialization.SerializationFig;
import org.apache.usergrid.persistence.core.astyanax.CassandraFig;
import org.apache.usergrid.persistence.core.guice.V3Impl;
import org.apache.usergrid.persistence.core.test.ITRunner;
import org.apache.usergrid.persistence.core.test.UseModules;
import org.junit.runner.RunWith;

/**
 * Classy class class.
 */


@RunWith( ITRunner.class )
@UseModules( TestCollectionModule.class )
public class MvccEntitySerializationStrategyV3ImplTest extends MvccEntitySerializationStrategyV3Test {
    @Inject
    @V3Impl
    private MvccEntitySerializationStrategy serializationStrategy;


    @Override
    protected MvccEntitySerializationStrategy getMvccEntitySerializationStrategy() {
        return serializationStrategy;
    }

}
