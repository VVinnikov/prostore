package ru.ibs.dtm.query.execution.core.dao;

import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;

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
