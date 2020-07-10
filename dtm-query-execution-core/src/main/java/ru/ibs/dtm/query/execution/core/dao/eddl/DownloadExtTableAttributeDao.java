package ru.ibs.dtm.query.execution.core.dao.eddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.query.execution.core.dto.edml.DownloadExternalTableAttribute;

import java.util.List;

public interface DownloadExtTableAttributeDao {

    void findDownloadExtTableAttributes(Long detId, Handler<AsyncResult<List<DownloadExternalTableAttribute>>> resultHandler);

    void dropDownloadExtTableAttributesByTableId(Long downloadExtTableId, Handler<AsyncResult<Integer>> handler);
}
