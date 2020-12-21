package io.arenadata.dtm.query.execution.plugin.adg.service;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Tarantool client pool
 */
public class AdgClientPool extends GenericObjectPool<AdgClient> {

    public AdgClientPool(PooledObjectFactory<AdgClient> factory) {
        super(factory);
    }

    public AdgClientPool(PooledObjectFactory<AdgClient> factory, GenericObjectPoolConfig<AdgClient> config) {
        super(factory, config);
        setTestOnBorrow(true);
    }

    public AdgClientPool(PooledObjectFactory<AdgClient> factory, GenericObjectPoolConfig<AdgClient> config, AbandonedConfig abandonedConfig) {
        super(factory, config, abandonedConfig);
    }
}
