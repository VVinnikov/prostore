package io.arenadata.dtm.query.execution.core.service.rollback;

import io.arenadata.dtm.query.execution.core.dto.edml.EraseWriteOpResult;
import io.vertx.core.Future;

import java.util.List;

public interface RestoreStateService {

    Future<Void> restoreState();

    Future<List<EraseWriteOpResult>> restoreErase(String datamart);

    Future<Void> restoreUpload(String datamart);
}
