package ru.ibs.dtm.query.execution.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TableAttribute implements Serializable {
    /**
     * Uuid
     */
    private UUID id;
    /**
     * Имя атрибута
     */
    private String mnemonic;
    /**
     * Тип атрибута
     */
    private AttributeType type;
    /**
     * Длина атрибута
     */
    private Integer length;
    /**
     * Размерность атрибута
     */
    private Integer accuracy;
    /**
     * Порядковый номер первичного ключа
     */
    private Integer primaryKeyOrder;
    /**
     * Порядковый номер distribute ключа
     */
    private Integer distributeKeyOrder;

    private int ordinalPosition;

    private Boolean nullable;
}
