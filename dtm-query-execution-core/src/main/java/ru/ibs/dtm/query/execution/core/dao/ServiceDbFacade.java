package ru.ibs.dtm.query.execution.core.dao;

import ru.ibs.dtm.query.execution.core.dao.ddl.DdlServiceDao;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.EddlServiceDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;

public interface ServiceDbFacade {

    /**
     * Dao service for interaction with metadata objects (datamarts, tables, views, attributes, types, etc...)
     * @return
     */
    ServiceDbDao getServiceDbDao();

    /**
     * Dao service for executing DDL actions in serviceDB
     * @return
     */
    DdlServiceDao getDdlServiceDao();

    /**
     * Dao service for interaction with EDDL objects
     * @return
     */
    EddlServiceDao getEddlServiceDao();

    /**
     * Dao service for interaction with delta objects
     * @return
     */
    DeltaServiceDao getDeltaServiceDao();
}
