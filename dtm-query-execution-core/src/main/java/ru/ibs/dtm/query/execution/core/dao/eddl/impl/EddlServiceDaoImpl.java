package ru.ibs.dtm.query.execution.core.dao.eddl.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.core.dao.eddl.*;

@Service
public class EddlServiceDaoImpl implements EddlServiceDao {

    private final DownloadExtTableDao downloadExtTableDao;
    private final UploadExtTableDao uploadExtTableDao;
    private final UploadQueryDao uploadQueryDao;
    private final DownloadQueryDao downloadQueryDao;
    private final DownloadExtTableAttributeDao downloadExtTableAttributeDao;

    @Autowired
    public EddlServiceDaoImpl(DownloadExtTableDao downloadExtTableDao, UploadExtTableDao uploadExtTableDao,
                              UploadQueryDao uploadQueryDao, DownloadQueryDao downloadQueryDao,
                              DownloadExtTableAttributeDao downloadExtTableAttributeDao) {
        this.downloadExtTableDao = downloadExtTableDao;
        this.uploadExtTableDao = uploadExtTableDao;
        this.uploadQueryDao = uploadQueryDao;
        this.downloadQueryDao = downloadQueryDao;
        this.downloadExtTableAttributeDao = downloadExtTableAttributeDao;
    }

    @Override
    public DownloadExtTableDao getDownloadExtTableDao() {
        return downloadExtTableDao;
    }

    @Override
    public UploadExtTableDao getUploadExtTableDao() {
        return uploadExtTableDao;
    }

    @Override
    public UploadQueryDao getUploadQueryDao() {
        return uploadQueryDao;
    }

    @Override
    public DownloadQueryDao getDownloadQueryDao() {
        return downloadQueryDao;
    }

    @Override
    public DownloadExtTableAttributeDao getDownloadExtTableAttributeDao() {
        return downloadExtTableAttributeDao;
    }
}
