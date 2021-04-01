package io.arenadata.dtm.query.execution.core.rollback.service;

import io.arenadata.dtm.query.execution.core.edml.dto.EraseWriteOpResult;
import io.vertx.core.Future;

import java.util.List;

public interface RestoreStateService {

    Future<Void> restoreState();

    Future<List<EraseWriteOpResult>> restoreErase(String datamart);

    Future<Void> restoreUpload(String datamart);
}
