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
package io.arenadata.dtm.query.execution.plugin.adb.configuration;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.GreenplumProperties;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryConfiguration {

  @Bean("adbQueryExecutor")
  public AdbQueryExecutor greenplam(@Value("${core.env.name}") String database, // Todo transfer to EnvProperties
                                    GreenplumProperties greenplumProperties,
                                    @Qualifier("adbTypeToSqlTypeConverter") SqlTypeConverter typeConverter,
                                    @Qualifier("adbTypeFromSqlTypeConverter") SqlTypeConverter sqlTypeConverter) {
    PgPoolOptions poolOptions = new PgPoolOptions();
    poolOptions.setDatabase(database);
    poolOptions.setHost(greenplumProperties.getHost());
    poolOptions.setPort(greenplumProperties.getPort());
    poolOptions.setUser(greenplumProperties.getUser());
    poolOptions.setPassword(greenplumProperties.getPassword());
    poolOptions.setMaxSize(greenplumProperties.getMaxSize());
    PgPool pgPool = PgClient.pool(poolOptions);
    return new AdbQueryExecutor(pgPool, greenplumProperties.getFetchSize(), typeConverter, sqlTypeConverter);
  }
}
