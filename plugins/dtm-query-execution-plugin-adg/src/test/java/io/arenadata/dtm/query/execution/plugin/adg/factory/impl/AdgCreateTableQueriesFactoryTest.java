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
package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.dto.AdgTables;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.AdgSpace;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.Space;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.SpaceIndex;
import io.arenadata.dtm.query.execution.plugin.adg.utils.TestUtils;
import io.arenadata.dtm.query.execution.plugin.api.factory.CreateTableQueriesFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AdgCreateTableQueriesFactoryTest {

    private AdgTables<AdgSpace> adgTables;
    private Map<String, Space> spaces;

    @BeforeEach
    void setUp() {
        Entity entity = TestUtils.getEntity();

        CreateTableQueriesFactory<AdgTables<AdgSpace>> adgCreateTableQueriesFactory =
                new AdgCreateTableQueriesFactory(new AdgTableEntitiesFactory(new TarantoolDatabaseProperties()));
        adgTables = adgCreateTableQueriesFactory.create(entity, "env");
        spaces = TestUtils.getSpaces(entity);
    }

    @Test
    void testActual() {
        testSpace(adgTables.getActual());
    }

    @Test
    void testHistory() {
        testSpace(adgTables.getHistory());
    }

    @Test
    void testStaging() {
        testSpace(adgTables.getStaging());
    }

    private void testSpace(AdgSpace testSpace) {
        Space space = spaces.get(testSpace.getName());
        assertNotNull(space);

        assertEquals(space.getFormat(), testSpace.getSpace().getFormat());

        List<String> testIndexNames = testSpace.getSpace().getIndexes().stream()
                .map(SpaceIndex::getName)
                .collect(Collectors.toList());
        List<String> indexNames = space.getIndexes().stream()
                .map(SpaceIndex::getName)
                .collect(Collectors.toList());
        assertEquals(indexNames, testIndexNames);
    }
}
