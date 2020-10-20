package ru.ibs.dtm.query.execution.plugin.adb.factory;

import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.PreparedStatementRequest;
import ru.ibs.dtm.query.execution.plugin.api.request.RollbackRequest;

import java.util.List;

public interface RollbackRequestFactory {

    List<PreparedStatementRequest> create(RollbackRequest rollbackRequest);
}
