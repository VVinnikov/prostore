package io.arenadata.dtm.query.execution.plugin.adg.service;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Реализация пула клиентов Tarantool
 */
public class TtPool extends GenericObjectPool<TtClient> {

  public TtPool(PooledObjectFactory<TtClient> factory) {
    super(factory);
  }

  public TtPool(PooledObjectFactory<TtClient> factory, GenericObjectPoolConfig<TtClient> config) {
    super(factory, config);
    setTestOnBorrow(true);
  }

  public TtPool(PooledObjectFactory<TtClient> factory, GenericObjectPoolConfig<TtClient> config, AbandonedConfig abandonedConfig) {
    super(factory, config, abandonedConfig);
  }
}
