package io.arenadata.dtm.common.transformer;

public interface Transformer<IN, OUT> {
    OUT transform(IN input);
}
