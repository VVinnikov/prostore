package io.arenadata.dtm.jdbc.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlParser {

    public static List<Query> parseSql(String query) throws SQLException {
        int inParen = 0;
        char[] aChars = query.toCharArray();
        boolean whitespaceOnly = true;
        List<Query> nativeQueries = null;
        StringBuilder nativeSql = new StringBuilder();
        int fragmentStart = 0;
        if (query.isEmpty()) {
            return Collections.singletonList(new Query(query, true));
        }

        for (int i = 0; i < aChars.length; ++i) {
            char aChar = aChars[i];
            whitespaceOnly &= aChar == ';' || Character.isWhitespace(aChar);
            switch (aChar) {
                case '"':
                    i = parseDoubleQuotes(aChars, i);
                    break;
                case '\'':
                    i = parseSingleQuotes(aChars, i);
                    break;
                case '(':
                    ++inParen;
                    break;
                case ')':
                    --inParen;
                    break;
                case ';':
                    if (inParen == 0) {
                        if (!whitespaceOnly) {
                            nativeSql.append(aChars, fragmentStart, i - fragmentStart);
                            whitespaceOnly = true;
                        }
                    } else {
                        throw new SQLException(String.format("Invalid sql query %s", query));
                    }
                    fragmentStart = i + 1;
                    if (nativeQueries == null) {
                        nativeQueries = new ArrayList();
                    }
                    nativeQueries.add(new Query(nativeSql.toString(), false));
                    nativeSql.setLength(0);
                    break;
            }
        }

        if (fragmentStart < aChars.length && !whitespaceOnly) {
            nativeSql.append(aChars, fragmentStart, aChars.length - fragmentStart);
        }

        if (nativeSql.length() == 0) {
            return nativeQueries != null ? nativeQueries : Collections.emptyList();
        } else {
            Query lastQuery = new Query(nativeSql.toString(), false);
            if (nativeQueries == null) {
                return Collections.singletonList(lastQuery);
            } else {
                if (!whitespaceOnly) {
                    nativeQueries.add(lastQuery);
                }
                return nativeQueries;
            }
        }
    }

    private static int parseSingleQuotes(char[] query, int offset) {
        while (true) {
            ++offset;
            if (offset >= query.length) {
                break;
            }

            if (query[offset] == '\'') {
                return offset;
            }
        }
        return query.length;
    }

    private static int parseDoubleQuotes(char[] query, int offset) {
        do {
            ++offset;
        } while (offset < query.length && query[offset] != '"');

        return offset;
    }
}
