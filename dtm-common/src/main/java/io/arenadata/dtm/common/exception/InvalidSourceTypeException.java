package io.arenadata.dtm.common.exception;

import io.arenadata.dtm.common.reader.SourceType;

import java.util.Arrays;
import java.util.stream.Collectors;

public class InvalidSourceTypeException extends RuntimeException {

    private static final String AVAILABLE_SOURCE_TYPES = Arrays.stream(SourceType.values())
            .filter(SourceType::isAvailable)
            .map(SourceType::name)
            .collect(Collectors.joining(", "));

    private static final String PATTERN = "\"%s\" isn't a valid datasource type," +
            " please use one of the following: " + AVAILABLE_SOURCE_TYPES;

    public InvalidSourceTypeException(String sourceType) {
        super(String.format(PATTERN, sourceType));
    }

    public InvalidSourceTypeException(String sourceType, Throwable cause) {
        super(String.format(PATTERN, sourceType), cause);
    }
}
