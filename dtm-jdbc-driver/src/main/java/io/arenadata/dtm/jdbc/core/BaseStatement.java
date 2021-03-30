package io.arenadata.dtm.jdbc.core;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public interface BaseStatement extends Statement {

    ResultSet createDriverResultSet(Field[] fields, List<Tuple> tuples);
}
