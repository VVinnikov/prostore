package ru.ibs.dtm.query.execution.core.dao.servicedb.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.core.dao.servicedb.*;

@Service
public class ServiceDbDaoImpl implements ServiceDbDao {

    private DatamartDao datamartDao;
    private EntityDao entityDao;
    private AttributeDao attributeDao;
    private AttributeTypeDao attributeTypeDao;
    private ViewDao viewDao;

    @Autowired
    public ServiceDbDaoImpl(DatamartDao datamartDao, EntityDao entityDao, AttributeDao attributeDao,
                            AttributeTypeDao attributeTypeDao, ViewDao viewDao) {
        this.datamartDao = datamartDao;
        this.entityDao = entityDao;
        this.attributeDao = attributeDao;
        this.attributeTypeDao = attributeTypeDao;
        this.viewDao = viewDao;
    }

    @Override
    public DatamartDao getDatamartDao() {
        return datamartDao;
    }

    @Override
    public EntityDao getEntityDao() {
        return entityDao;
    }

    @Override
    public AttributeDao getAttributeDao() {
        return attributeDao;
    }

    @Override
    public AttributeTypeDao getAttributeTypeDao() {
        return attributeTypeDao;
    }

    @Override
    public ViewDao getViewServiceDao() {
        return viewDao;
    }
}
