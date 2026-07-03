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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SqlUtilsTest {

  @ParameterizedTest(name = "{0}")
  @CsvSource({"flink.sql,18", "test_sql.sql,6", "test_udf_sql.sql,6"})
  void givenSource_when_thenSplitCorrectly(String filename, int numberOfStatements)
      throws Exception {
    var script = Resources.toString(getClass().getResource("/sql/" + filename), Charsets.UTF_8);
    var stmts = SqlUtils.parseStatements(script);
    assertThat(stmts).isNotNull().isNotEmpty().hasSize(numberOfStatements);
  }

  @Test
  void givenDoubleDashInsideStringLiteral_whenParse_thenNotStrippedAsComment() {
    var script =
        "CREATE TEMPORARY TABLE dest (id INT) WITH (\n"
            + "  'password' = 'Barbarr0206---',\n"
            + "  'connector' = 'redis'\n"
            + ");";

    var stmts = SqlUtils.parseStatements(script);

    assertThat(stmts).hasSize(1);
    assertThat(stmts.get(0))
        .contains("'password' = 'Barbarr0206---'")
        .contains("'connector' = 'redis'");
  }

  @Test
  void givenBlockCommentMarkersInsideStringLiteral_whenParse_thenNotStripped() {
    var script = "CREATE TEMPORARY TABLE t (id INT) WITH (\n" + "  'note' = 'a/*b*/c'\n" + ");";

    var stmts = SqlUtils.parseStatements(script);

    assertThat(stmts).hasSize(1);
    assertThat(stmts.get(0)).contains("'note' = 'a/*b*/c'");
  }

  @Test
  void givenEscapedQuoteInsideStringLiteral_whenParse_thenDoubleDashStillPreserved() {
    // The '' is an escaped single quote inside the literal; the -- after it is still in-string.
    var script = "CREATE TEMPORARY TABLE t (id INT) WITH (\n" + "  'p' = 'ab''cd--ef'\n" + ");";

    var stmts = SqlUtils.parseStatements(script);

    assertThat(stmts).hasSize(1);
    assertThat(stmts.get(0)).contains("'p' = 'ab''cd--ef'");
  }

  @Test
  void givenCertificateInsideStringLiteral_whenParse_thenPreserved() {
    var script =
        "CREATE TEMPORARY TABLE t (id INT) WITH (\n"
            + "  'ssl.trust' = '-----BEGIN CERTIFICATE-----MIIabc-----END CERTIFICATE-----'\n"
            + ");";

    var stmts = SqlUtils.parseStatements(script);

    assertThat(stmts).hasSize(1);
    assertThat(stmts.get(0))
        .contains("'-----BEGIN CERTIFICATE-----MIIabc-----END CERTIFICATE-----'");
  }

  @Test
  void givenLineComment_whenParse_thenStripped() {
    var script =
        "-- leading comment\n"
            + "CREATE TABLE t (id INT) WITH (\n"
            + "  'connector' = 'print' --trailing comment\n"
            + ");";

    var stmts = SqlUtils.parseStatements(script);

    assertThat(stmts).hasSize(1);
    assertThat(stmts.get(0)).doesNotContain("leading comment").doesNotContain("trailing comment");
    assertThat(stmts.get(0)).contains("'connector' = 'print'");
  }

  @Test
  void givenBlockComment_whenParse_thenStripped() {
    var script =
        "CREATE TABLE t /* inline block comment */ (id INT) WITH (\n" + "  'k' = 'v'\n" + ");";

    var stmts = SqlUtils.parseStatements(script);

    assertThat(stmts).hasSize(1);
    assertThat(stmts.get(0)).doesNotContain("inline block comment");
    assertThat(stmts.get(0)).contains("'k' = 'v'");
  }
}
