package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Space index
 *
 * @unique is unique
 * @parts parts of composite index
 * @parts index type
 * @parts name
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpaceIndex {
    Boolean unique;
    List<SpaceIndexPart> parts;
    SpaceIndexTypes type;
    String name;
}
