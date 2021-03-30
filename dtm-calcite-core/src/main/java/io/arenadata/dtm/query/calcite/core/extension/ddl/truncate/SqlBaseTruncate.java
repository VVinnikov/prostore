package io.arenadata.dtm.query.calcite.core.extension.ddl.truncate;

import io.arenadata.dtm.common.ddl.TruncateType;

public interface SqlBaseTruncate {
    TruncateType getTruncateType();
}
