package io.arenadata.dtm.query.execution.core.base.repository;

import io.arenadata.dtm.query.execution.core.delta.repository.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.ServiceDbDao;

public interface ServiceDbFacade {

    /**
     * Dao service for interaction with metadata objects (datamarts, tables, views, attributes, types, etc...)
     *
     * @return
     */
    ServiceDbDao getServiceDbDao();

    /**
     * Dao service for interaction with delta objects
     *
     * @return
     */
    DeltaServiceDao getDeltaServiceDao();
}
