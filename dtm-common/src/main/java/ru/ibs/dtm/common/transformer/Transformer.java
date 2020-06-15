package ru.ibs.dtm.common.transformer;

public interface Transformer<IN, OUT> {
    OUT transform(IN input);
}
