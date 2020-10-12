package ru.ibs.dtm.query.execution.core.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.core.dao.ddl.DdlServiceDao;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dao.eddl.EddlServiceDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;

@Service
public class ServiceDbFacadeImpl implements ServiceDbFacade {

    private final ServiceDbDao serviceDbDao;
    private final DdlServiceDao ddlServiceDao;
    private final EddlServiceDao eddlServiceDao;
    private final DeltaServiceDao deltaServiceDao;

    @Autowired
    public ServiceDbFacadeImpl(ServiceDbDao serviceDbDao, DdlServiceDao ddlServiceDao, EddlServiceDao eddlServiceDao,
                               DeltaServiceDao deltaServiceDao) {
        this.serviceDbDao = serviceDbDao;
        this.ddlServiceDao = ddlServiceDao;
        this.eddlServiceDao = eddlServiceDao;
        this.deltaServiceDao = deltaServiceDao;
    }

    @Override
    public ServiceDbDao getServiceDbDao() {
        return serviceDbDao;
    }

    @Override
    public DdlServiceDao getDdlServiceDao() {
        return ddlServiceDao;
    }

    @Override
    public EddlServiceDao getEddlServiceDao() {
        return eddlServiceDao;
    }

    @Override
    public DeltaServiceDao getDeltaServiceDao() {
        return deltaServiceDao;
    }
}
