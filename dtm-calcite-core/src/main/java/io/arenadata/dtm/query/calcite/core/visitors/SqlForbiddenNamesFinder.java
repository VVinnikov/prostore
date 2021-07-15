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
package io.arenadata.dtm.query.calcite.core.visitors;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SqlForbiddenNamesFinder extends SqlBasicVisitor<Object> {
    private static final Set<String> SYSTEM_FORBIDDEN_NAMES = new HashSet<>(Arrays.asList("sys_op", "sys_from", "sys_to",
            "sys_close_date", "bucket_id", "sign"));
    private final Set<String> foundForbiddenNames = new HashSet<>();

    @Override
    public Object visit(SqlIdentifier id) {
        id.names.forEach(name -> {
            if (SYSTEM_FORBIDDEN_NAMES.contains(name)) {
                foundForbiddenNames.add(name);
            }
        });
        return null;
    }

    public Set<String> getFoundForbiddenNames() {
        return foundForbiddenNames;
    }
}
