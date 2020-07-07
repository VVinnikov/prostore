package ru.ibs.dtm.liquibase.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LiquibaseContext {
    private LiquibaseCommand command;
}
