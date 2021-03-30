/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.core.service.eddl.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlAction;
import io.arenadata.dtm.query.execution.core.dto.eddl.EddlQuery;
import io.arenadata.dtm.query.execution.core.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.service.eddl.EddlExecutor;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DropUploadExternalTableExecutor implements EddlExecutor {

    private final CacheService<EntityKey, Entity> entityCacheService;
    private final EntityDao entityDao;

    @Autowired
    public DropUploadExternalTableExecutor(@Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                           ServiceDbFacade serviceDbFacade) {
        this.entityCacheService = entityCacheService;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public Future<QueryResult> execute(EddlQuery query) {
        return Future.future(promise -> {
            DropUploadExternalTableQuery castQuery = (DropUploadExternalTableQuery) query;
            val datamartName = castQuery.getSchemaName();
            val entityName = castQuery.getTableName();
            evictEntityCache(datamartName, entityName);
            dropTable(datamartName, entityName)
                    .onSuccess(r -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    protected Future<Void> dropTable(String datamartName, String entityName) {
        return getEntity(datamartName, entityName)
                .compose(this::dropEntityIfExists);
    }

    private void evictEntityCache(String datamartName, String entityName) {
        entityCacheService.remove(new EntityKey(datamartName, entityName));
    }

    private Future<Entity> getEntity(String datamartName, String entityName) {
        return Future.future(entityPromise -> {
            val tableWithSchema = datamartName + "." + entityName;
            entityDao.getEntity(datamartName, entityName)
                    .onSuccess(entity -> {
                        if (EntityType.UPLOAD_EXTERNAL_TABLE == entity.getEntityType()) {
                            entityPromise.complete(entity);
                        } else {
                            entityPromise.fail(new EntityNotExistsException(tableWithSchema));
                        }
                    })
                    .onFailure(entityPromise::fail);
        });
    }

    private Future<Void> dropEntityIfExists(Entity entity) {
        if (entity != null) {
            return entityDao.deleteEntity(entity.getSchema(), entity.getName());
        } else {
            return Future.succeededFuture();
        }
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.DROP_UPLOAD_EXTERNAL_TABLE;
    }
}
