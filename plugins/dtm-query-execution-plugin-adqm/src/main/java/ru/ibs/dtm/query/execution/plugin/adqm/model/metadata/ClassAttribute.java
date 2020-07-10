package ru.ibs.dtm.query.execution.plugin.adqm.model.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ClassAttribute {
    private UUID id;
    /*Имя атрибута*/
    private String mnemonic;
    /*Тип атрибута*/
    private TypeMessage type;
}