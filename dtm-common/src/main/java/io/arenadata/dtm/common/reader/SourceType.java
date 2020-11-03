package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.common.exception.InvalidSourceTypeException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;

/**
 * Data source type
 */

@NoArgsConstructor
@AllArgsConstructor
public enum SourceType {
    ADB,
    ADG,
    ADQM,
    INFORMATION_SCHEMA(false);

    private @Getter boolean isAvailable = true;

    public static SourceType valueOfAvailable(String typeName) {
        return Arrays.stream(SourceType.values())
                .filter(type -> type.isAvailable() && type.name().equalsIgnoreCase(typeName))
                .findAny()
                .orElseThrow(() -> new InvalidSourceTypeException(typeName));
    }
}
