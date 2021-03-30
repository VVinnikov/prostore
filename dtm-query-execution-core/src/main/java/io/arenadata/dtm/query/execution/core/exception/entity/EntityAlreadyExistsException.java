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
package io.arenadata.dtm.query.execution.core.exception.entity;

import io.arenadata.dtm.common.exception.DtmException;

public class EntityAlreadyExistsException extends DtmException {

    public EntityAlreadyExistsException(String table) {
        super(String.format("Entity %s already exists", table));
    }

    public EntityAlreadyExistsException(String table, Throwable cause) {
        super(String.format("Entity %s already exists", table), cause);
    }
}
