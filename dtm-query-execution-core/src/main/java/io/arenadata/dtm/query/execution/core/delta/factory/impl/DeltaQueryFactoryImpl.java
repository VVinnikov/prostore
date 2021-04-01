package io.arenadata.dtm.query.execution.core.delta.factory.impl;

import io.arenadata.dtm.query.calcite.core.extension.delta.SqlBeginDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlCommitDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlRollbackDelta;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaByDateTime;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaByNum;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaHot;
import io.arenadata.dtm.query.calcite.core.extension.delta.function.SqlGetDeltaOk;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.delta.dto.operation.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.delta.dto.query.*;
import io.arenadata.dtm.query.execution.core.delta.factory.DeltaQueryFactory;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

import static io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil.DELTA_DATE_TIME_FORMATTER;
import static io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil.DELTA_DATE_TIME_PATTERN;

@Component
@Slf4j
public class DeltaQueryFactoryImpl implements DeltaQueryFactory {

    private final SqlDialect sqlDialect;

    @Autowired
    public DeltaQueryFactoryImpl(@Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public DeltaQuery create(DeltaRequestContext context) {
        val sqlNode = context.getSqlNode();
        if (context.getSqlNode() instanceof SqlBeginDelta) {
            return BeginDeltaQuery.builder()
                    .deltaNum(((SqlBeginDelta) sqlNode).getDeltaNumOperator().getNum())
                    .build();
        } else if (sqlNode instanceof SqlCommitDelta) {
            return CommitDeltaQuery.builder()
                    .deltaDate(getDeltaDateTime(((SqlCommitDelta) sqlNode).getDeltaDateTimeOperator().getDeltaDateTime()))
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaOk) {
            return GetDeltaOkQuery.builder()
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaHot) {
            return GetDeltaHotQuery.builder()
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaByNum) {
            return GetDeltaByNumQuery.builder()
                    .deltaNum(((SqlGetDeltaByNum) sqlNode).getDeltaNum())
                    .build();
        } else if (sqlNode instanceof SqlGetDeltaByDateTime) {
            return GetDeltaByDateTimeQuery.builder()
                    .deltaDate(getDeltaDateTime(((SqlGetDeltaByDateTime) sqlNode).getDeltaDateTime()))
                    .build();
        } else if (sqlNode instanceof SqlRollbackDelta) {
            return RollbackDeltaQuery.builder()
                    .sqlNode((SqlRollbackDelta) sqlNode)
                    .envName(context.getEnvName())
                    .build();
        } else {
            throw new DtmException(String.format("Query [%s] is not a DELTA operator",
                    sqlNode.toSqlString(sqlDialect)));
        }
    }

    private LocalDateTime getDeltaDateTime(String deltaDateTimeStr) {
        if (deltaDateTimeStr != null) {
            try {
                return LocalDateTime.parse(deltaDateTimeStr, DELTA_DATE_TIME_FORMATTER);
            } catch (Exception e) {
                throw new DtmException(String.format("Incorrect format of delta date value: %s, correct template: %s",
                        deltaDateTimeStr, DELTA_DATE_TIME_PATTERN), e);
            }
        } else {
            return null;
        }
    }
}
