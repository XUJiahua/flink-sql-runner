/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.flinkrunner;

import java.util.ArrayList;
import java.util.List;

/** Utility class for parsing SQL scripts. */
final class SqlUtils {

  private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
  private static final String LINE_DELIMITER = "\n";

  /**
   * Parses SQL statements from a script.
   *
   * @param script The SQL script content.
   * @return A list of individual SQL statements.
   */
  static List<String> parseStatements(String script) {
    var formatted = removeComments(formatSqlFile(script));

    var statements = new ArrayList<String>();

    StringBuilder current = null;
    var statementSet = false;
    for (String line : formatted.split("\n")) {
      var trimmed = line.trim();
      if (trimmed.isBlank()) {
        continue;
      }
      if (current == null) {
        current = new StringBuilder();
      }
      if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
        statementSet = true;
      }
      current.append(trimmed);
      current.append("\n");
      if (trimmed.endsWith(STATEMENT_DELIMITER)) {
        if (!statementSet || trimmed.equalsIgnoreCase("END;")) {
          statements.add(current.toString());
          current = null;
          statementSet = false;
        }
      }
    }
    return statements;
  }

  /**
   * Removes SQL comments ({@code --} line comments and {@code /* *}{@code /} block comments) while
   * preserving the content of string literals and quoted identifiers.
   *
   * <p>The previous implementation used a plain regular expression that was not aware of string
   * literals. As a result, any occurrence of {@code --} (or {@code /*}) inside a quoted value - for
   * example a Redis password such as {@code 'secret---'} or a PEM certificate - was treated as a
   * comment and silently stripped, producing an unterminated string literal and a parser error.
   * This scanner only treats comment markers as comments when they appear outside of a quoted
   * region.
   *
   * @param sql the SQL content to strip comments from
   * @return the SQL content with comments removed and all quoted content preserved
   */
  static String removeComments(String sql) {
    var result = new StringBuilder(sql.length());
    var length = sql.length();
    var i = 0;

    while (i < length) {
      var c = sql.charAt(i);

      // Single-quoted string literal or backtick-quoted identifier: copy verbatim.
      if (c == '\'' || c == '`') {
        i = copyQuoted(sql, i, c, result);
        continue;
      }

      // Line comment: skip everything up to (but not including) the end of the line.
      if (c == '-' && i + 1 < length && sql.charAt(i + 1) == '-') {
        i += 2;
        while (i < length && sql.charAt(i) != '\n') {
          i++;
        }
        continue;
      }

      // Block comment: skip everything up to and including the closing marker.
      if (c == '/' && i + 1 < length && sql.charAt(i + 1) == '*') {
        i += 2;
        while (i < length
            && !(sql.charAt(i) == '*' && i + 1 < length && sql.charAt(i + 1) == '/')) {
          i++;
        }
        i = Math.min(i + 2, length); // skip the closing "*/" if present
        continue;
      }

      result.append(c);
      i++;
    }

    return result.toString();
  }

  /**
   * Copies a quoted region (string literal or quoted identifier) verbatim into {@code result},
   * honouring the SQL doubling escape (e.g. {@code ''} inside a single-quoted literal).
   *
   * @param sql the full SQL content
   * @param start index of the opening quote character
   * @param quote the quote character that opened the region
   * @param result the buffer to append the copied characters to
   * @return the index of the first character after the closing quote (or end of input)
   */
  private static int copyQuoted(String sql, int start, char quote, StringBuilder result) {
    var length = sql.length();
    result.append(quote);
    var i = start + 1;

    while (i < length) {
      var d = sql.charAt(i);
      result.append(d);
      i++;

      if (d == quote) {
        // A doubled quote is an escaped quote and does not close the region.
        if (i < length && sql.charAt(i) == quote) {
          result.append(quote);
          i++;
        } else {
          break;
        }
      }
    }

    return i;
  }

  /**
   * Formats the SQL file content to ensure proper statement termination.
   *
   * @param content The SQL file content.
   * @return Formatted SQL content.
   */
  static String formatSqlFile(String content) {
    var trimmed = content.trim();
    var formatted = new StringBuilder();
    formatted.append(trimmed);
    if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
      formatted.append(STATEMENT_DELIMITER);
    }
    formatted.append(LINE_DELIMITER);
    return formatted.toString();
  }

  private SqlUtils() {
    throw new UnsupportedOperationException();
  }
}
