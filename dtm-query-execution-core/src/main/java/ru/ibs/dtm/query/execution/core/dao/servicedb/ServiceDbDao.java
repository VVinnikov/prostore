package ru.ibs.dtm.query.execution.core.dao.servicedb;

public interface ServiceDbDao {

    DatamartDao getDatamartDao();

    EntityDao getEntityDao();

    AttributeDao getAttributeDao();

    AttributeTypeDao getAttributeTypeDao();

    ViewDao getViewServiceDao();
}
