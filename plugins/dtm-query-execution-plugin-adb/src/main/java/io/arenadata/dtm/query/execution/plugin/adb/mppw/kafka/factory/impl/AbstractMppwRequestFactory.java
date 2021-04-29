package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;

import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.SYS_FROM_ATTR;

public abstract class AbstractMppwRequestFactory implements MppwRequestFactory<AdbKafkaMppwTransferRequest> {

    protected List<String> getStagingColumnList(MppwTransferDataRequest request) {
        return request.getColumnList().stream()
            .map(fieldName -> SYS_FROM_ATTR.equals(fieldName) ? String.valueOf(request.getHotDelta()) : fieldName)
            .collect(Collectors.toList());
    }
}
