package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.common.exception.InvalidSourceTypeException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Data source type
 */

@NoArgsConstructor
@AllArgsConstructor
public enum SourceType {
    ADB,
    ADG,
    ADQM,
    ADP,
    INFORMATION_SCHEMA(false);

    @Getter
    private boolean isAvailable = true;

    public static SourceType valueOfAvailable(String typeName) {
        return Arrays.stream(SourceType.values())
                .filter(type -> type.isAvailable() && type.name().equalsIgnoreCase(typeName))
                .findAny()
                .orElseThrow(() -> new InvalidSourceTypeException(typeName));
    }

    public static Set<SourceType> pluginsSourceTypes() {
        return Arrays.stream(SourceType.values())
                .filter(st -> st != SourceType.INFORMATION_SCHEMA)
                .collect(Collectors.toSet());
    }
}
