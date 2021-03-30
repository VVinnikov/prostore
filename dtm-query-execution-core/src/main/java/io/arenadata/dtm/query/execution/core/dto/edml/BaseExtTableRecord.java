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
package io.arenadata.dtm.query.execution.core.dto.edml;

import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.plugin.exload.Type;
import io.vertx.core.json.JsonObject;
import lombok.Data;

@Data
public class BaseExtTableRecord {
    private Long id;
    private Long datamartId;
    private String tableName;
    private Type locationType;
    private String locationPath;
    private ExternalTableFormat format;
    private JsonObject tableSchema;
}
