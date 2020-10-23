package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;

import java.util.List;

public interface MppwRequestFactory {

	List<PreparedStatementRequest> create(MppwTransferDataRequest request);
}
