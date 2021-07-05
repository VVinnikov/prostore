package io.arenadata.dtm.query.calcite.core.visitors;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SqlForbiddenNamesFinder extends SqlBasicVisitor<Object> {
    private static final Set<String> SYSTEM_FORBIDDEN_NAMES = new HashSet<>(Arrays.asList("sys_op", "sys_from", "sys_to",
            "sys_close_date", "bucket_id", "sign"));
    private final Set<String> foundForbiddenNames = new HashSet<>();

    @Override
    public Object visit(SqlIdentifier id) {
        id.names.forEach(name -> {
            if (SYSTEM_FORBIDDEN_NAMES.contains(name)) {
                foundForbiddenNames.add(name);
            }
        });
        return null;
    }

    public Set<String> getFoundForbiddenNames() {
        return foundForbiddenNames;
    }
}
