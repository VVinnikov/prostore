package ru.ibs.dtm.query.execution.plugin.adb.factory;

import ru.ibs.dtm.common.plugin.sql.PreparedStatementRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;

import java.util.List;

public interface MppwRequestFactory {

	List<PreparedStatementRequest> create(MppwTransferDataRequest request);
}
