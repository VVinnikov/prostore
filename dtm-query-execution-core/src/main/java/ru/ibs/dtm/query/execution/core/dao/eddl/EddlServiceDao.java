package ru.ibs.dtm.query.execution.core.dao.eddl;

public interface EddlServiceDao {

    DownloadExtTableDao getDownloadExtTableDao();

    UploadExtTableDao getUploadExtTableDao();

    UploadQueryDao getUploadQueryDao();

    DownloadQueryDao getDownloadQueryDao();

    DownloadExtTableAttributeDao getDownloadExtTableAttributeDao();
}
