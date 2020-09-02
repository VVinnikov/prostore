package ru.ibs.dtm.query.execution.plugin.adb.factory;

import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.PreparedStatementRequest;

import java.util.List;

public interface MppwRequestFactory {

	List<PreparedStatementRequest> create(MppwTransferDataRequest request);
}
