package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Adg space
 *
 * @format attributes
 * @temporary temporary
 * @engine engine
 * @isLocal local
 * @shardingKey key of sharding
 * @indexes indexes
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Space {
    List<SpaceAttribute> format;
    Boolean temporary;
    SpaceEngines engine;
    @JsonProperty("is_local")
    Boolean isLocal;
    @JsonProperty("sharding_key")
    List<String> shardingKey;
    List<SpaceIndex> indexes;
}
