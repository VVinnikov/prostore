package io.arenadata.dtm.query.execution.core.base.service.metadata.impl;

import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.dto.metadata.DatamartEntity;
import io.arenadata.dtm.query.execution.core.base.dto.metadata.DatamartInfo;
import io.arenadata.dtm.query.execution.core.base.dto.metadata.EntityAttribute;
import io.arenadata.dtm.query.execution.core.base.service.metadata.DatamartMetaService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DatamartMetaServiceImpl implements DatamartMetaService {

    private final DatamartDao datamartDao;
    private final EntityDao entityDao;

    public DatamartMetaServiceImpl(ServiceDbFacade serviceDbFacade) {
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public Future<List<DatamartInfo>> getDatamartMeta() {
        return datamartDao.getDatamartMeta();
    }

    @Override
    public Future<List<DatamartEntity>> getEntitiesMeta(String datamartMnemonic) {
        return entityDao.getEntitiesMeta(datamartMnemonic);
    }

    @Override
    public Future<List<EntityAttribute>> getAttributesMeta(String datamartMnemonic,
                                                           String entityMnemonic) {
        return entityDao.getEntity(datamartMnemonic, entityMnemonic)
                .map(entity -> entity.getFields().stream()
                        .map(ef -> EntityAttribute.builder()
                                .datamartMnemonic(datamartMnemonic)
                                .entityMnemonic(entityMnemonic)
                                .accuracy(ef.getAccuracy())
                                .distributeKeykOrder(ef.getShardingOrder())
                                .primaryKeyOrder(ef.getPrimaryOrder())
                                .dataType(ef.getType())
                                .length(ef.getSize())
                                .mnemonic(ef.getName())
                                .ordinalPosition(ef.getOrdinalPosition())
                                .nullable(ef.getNullable())
                                .accuracy(ef.getAccuracy())
                                .build())
                        .collect(Collectors.toList()));
    }
}
