package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.query.execution.plugin.adg.configuration.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtResultTranslator;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class TtClientFactory extends BasePooledObjectFactory<TtClient> {

  private TarantoolDatabaseProperties tarantoolProperties;
  private TtResultTranslator resultTranslator;

  public TtClientFactory(TarantoolDatabaseProperties tarantoolProperties, TtResultTranslator resultTranslator) {
    this.tarantoolProperties = tarantoolProperties;
    this.resultTranslator = resultTranslator;
  }

  @Override
  public TtClient create() {
    TtClient client;
    try {
      client = new TtClientImpl(tarantoolProperties, resultTranslator);
    } catch (Exception e) {
      throw new RuntimeException("Error connecting to Tarantool: " + tarantoolProperties, e);
    }
    return client;
  }

  @Override
  public PooledObject<TtClient> wrap(TtClient ttClient) {
    return new DefaultPooledObject<>(ttClient);
  }

  @Override
  public void destroyObject(PooledObject<TtClient> p) {
    p.getObject().close();
  }
}