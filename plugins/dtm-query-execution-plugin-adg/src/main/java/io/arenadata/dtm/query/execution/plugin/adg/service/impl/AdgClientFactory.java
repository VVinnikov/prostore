package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgResultTranslator;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class AdgClientFactory extends BasePooledObjectFactory<AdgClient> {

    private final TarantoolDatabaseProperties tarantoolProperties;
    private final AdgResultTranslator resultTranslator;

    public AdgClientFactory(TarantoolDatabaseProperties tarantoolProperties, AdgResultTranslator resultTranslator) {
        this.tarantoolProperties = tarantoolProperties;
        this.resultTranslator = resultTranslator;
    }

    @Override
    public AdgClient create() {
        AdgClient client;
        try {
            client = new AdgClientImpl(tarantoolProperties, resultTranslator);
        } catch (Exception e) {
            throw new DataSourceException(String.format("Error connecting to Tarantool: %s",
                    tarantoolProperties), e);
        }
        return client;
    }

    @Override
    public PooledObject<AdgClient> wrap(AdgClient adgClient) {
        return new DefaultPooledObject<>(adgClient);
    }

    @Override
    public void destroyObject(PooledObject<AdgClient> p) {
        p.getObject().close();
    }

    @Override
    public boolean validateObject(PooledObject<AdgClient> p) {
        return p.getObject().isAlive();
    }
}
