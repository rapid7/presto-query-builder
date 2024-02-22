// ===========================================================================
// COPYRIGHT (C) 2019, Rapid7 LLC, Boston, MA, USA.
// ---------------------------------------------------------------------------
// This code is licensed under MIT license (see LICENSE for details)
// ===========================================================================
package com.rapid7.presto;

import org.junit.jupiter.api.Test;

import static com.rapid7.presto.JoinType.INNER;
import static com.rapid7.presto.QueryUtils.aliasedTable;
import static com.rapid7.presto.QueryUtils.equal;
import static com.rapid7.presto.QueryUtils.lit;
import static com.rapid7.presto.QueryUtils.param;
import static com.rapid7.presto.QueryUtils.projection;
import static com.rapid7.presto.QueryUtils.ref;
import static com.rapid7.presto.QueryUtils.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryFactoryTest {
  @Test
  void mustSpecifySelect() {
    assertThrows(UnsupportedOperationException.class, () -> {
      // Given
      QueryFactory qf = new QueryFactory();

      // When
      qf.build(new SelectQueryBuilder());
    });
  }

  @Test
  void mustSpecifySelectWith() {
    assertThrows(UnsupportedOperationException.class, () -> {
      // Given
      QueryFactory qf = new QueryFactory();

      // When
      qf.build(new SelectQueryBuilder().with("alias").end());
    });
  }

  @Test
  void mustSpecifyFrom() {
    assertThrows(UnsupportedOperationException.class, () -> {
      // Given
      QueryFactory qf = new QueryFactory();

      // When
      qf.build(new SelectQueryBuilder().join(INNER, aliasedTable("t1", "b"), equal(ref("a.a"), ref("b.a"))));
    });
  }

  @Test
  void basicSelect() {
    // Given
    QueryFactory qf = new QueryFactory();

    // When
    String query = qf.build(new SelectQueryBuilder().select(false, projection("t1", ref("a.a"))));

    // Then
    assertEquals("SELECT a.a t1", query);
  }

  @Test
  void basicJoin() {
    // Given
    QueryFactory qf = new QueryFactory();

    // When
    String query = qf.build(
        new SelectQueryBuilder()
            .select(
                false,
                projection("t1a", ref("t1.a")),
                projection("t2a", ref("t2.a"))
            )
            .from(aliasedTable("t1", "a"))
            .join(INNER, aliasedTable("t2", "b"), equal(ref("t1.a"), ref("t2.a")))
    );

    // Then
    assertEquals(
        """
        SELECT
          t1.a t1a
        , t2.a t2a
        FROM
          (a t1
        INNER JOIN b t2 ON (t1.a = t2.a))\
        """,
        query
    );
  }

  @Test
  void basicWith() {
    // Given
    QueryFactory qf = new QueryFactory();

    // When
    String query = qf.build(
        new SelectQueryBuilder()
            .with("with_table")
            .select(
                false,
                projection("aliasa", ref("alias.a"))
            ).from(aliasedTable("alias", "b"))
            .end()
            .select(
                false,
                projection("t1a", ref("t1.a")),
                projection("t2aliasa", ref("t2.aliasa"))
            )
            .from(aliasedTable("t1", "a"))
            .join(INNER, aliasedTable("t2", "with_table"), equal(ref("t1.a"), ref("t2.aliasa")))
    );

    // Then
    assertEquals(
        """
        WITH
          with_table AS (
           SELECT alias.a aliasa
           FROM
             b alias
        )\s
        SELECT
          t1.a t1a
        , t2.aliasa t2aliasa
        FROM
          (a t1
        INNER JOIN with_table t2 ON (t1.a = t2.aliasa))\
        """,
        query
    );
  }

  @Test
  void multiWith() {
    // Given
    QueryFactory qf = new QueryFactory();

    // When
    String query = qf.build(
        new SelectQueryBuilder()
            .with("with_table_1")
            .select(
                false,
                projection("alias1a", ref("alias1.a"))
            ).from(aliasedTable("alias1", "b"))
            .end()
            .with("with_table_2")
            .select(
                false,
                projection("alias2a", ref("alias2.a"))
            ).from(aliasedTable("alias2", "c"))
            .end()
            .select(
                false,
                projection("t1a", ref("t1.a")),
                projection("t2aliasa", ref("t2.aliasa"))
            )
            .from(aliasedTable("t1", "a"))
            .join(INNER, aliasedTable("t2", "with_table"), equal(ref("t1.a"), ref("t2.aliasa")))
    );

    // Then
    assertEquals(
        """
        WITH
          with_table_1 AS (
           SELECT alias1.a alias1a
           FROM
             b alias1
        )\s
        , with_table_2 AS (
           SELECT alias2.a alias2a
           FROM
             c alias2
        )\s
        SELECT
          t1.a t1a
        , t2.aliasa t2aliasa
        FROM
          (a t1
        INNER JOIN with_table t2 ON (t1.a = t2.aliasa))\
        """,
        query
    );
  }

  @Test
  void basicParameter() {
    // Given
    QueryFactory qf = new QueryFactory();
    qf.setParam("id", lit("some_id"));

    // When
    String query = qf.build(
        new SelectQueryBuilder()
            .select(false, projection("t1a", ref("t1.a")))
            .from(aliasedTable("t1", "a"))
            .where(equal(ref("t1.id"), param(qf, "id")))
    );

    // Then
    assertEquals(
        """
        SELECT t1.a t1a
        FROM
          a t1
        WHERE (t1.id = 'some_id')\
        """,
        query
    );
  }

  @Test
  void multiWhere() {
    // Given
    QueryFactory qf = new QueryFactory();
    qf.setParam("id", lit("some_id"));

    // When
    String query = qf.build(
        new SelectQueryBuilder()
            .select(false, projection("t1a", ref("t1.a")))
            .from(aliasedTable("t1", "a"))
            .where(equal(ref("t1.id"), param(qf, "id")))
            .and(equal(ref("t1.name"), lit("some_name")))
            .and(equal(ref("t1.title"), lit("some_title")))
            .or(equal(ref("t1.id"), lit("other_id")))
    );

    // Then
    assertEquals(
        """
        SELECT t1.a t1a
        FROM
          a t1
        WHERE ((((t1.id = 'some_id') AND (t1.name = 'some_name')) AND (t1.title = 'some_title')) OR (t1.id = 'other_id'))\
        """,
        query
    );
  }

  @Test
  void multiDepthRef() {
    // Given
    QueryFactory qf = new QueryFactory();

    // When
    String query = qf.build(
        new SelectQueryBuilder().select(false, projection("abcd", ref("a-b.b-c.c-d")))
    );

    // Then
    assertEquals(
        "SELECT \"a-b\".\"b-c\".\"c-d\" abcd",
        query
    );
  }

  @Test
  void union() {
    // Given
    QueryFactory qf = new QueryFactory();

    // When
    String query = qf.build(
        new SelectQueryBuilder()
            .select(false, projection("a", ref("t1.a")))
            .from(aliasedTable("t1", "a"))
            .union()
            .select(false, projection("a", ref("t2.a")))
            .from(aliasedTable("t2", "b"))
    );

    // Then
    assertEquals(
        """
        SELECT t1.a a
        FROM
          a t1
        UNION ALL SELECT t2.a a
        FROM
          b t2\
        """,
        query
    );
  }

  @Test
  void unionWith() {
    // Given
    QueryFactory qf = new QueryFactory();

    // When
    String query = qf.build(
        new SelectQueryBuilder()
            .with("unioned")
            .select(false, projection("a", ref("t1.a")))
            .from(aliasedTable("t1", "a"))
            .union()
            .select(false, projection("a", ref("t2.a")))
            .from(aliasedTable("t2", "b"))
            .end()
            .select(false, projection("a", ref("u.a")))
            .from(aliasedTable("u", "unioned"))
    );

    // Then
    assertEquals(
        """
        WITH
          unioned AS (
           SELECT t1.a a
           FROM
             a t1
        UNION ALL    SELECT t2.a a
           FROM
             b t2
        )\s
        SELECT u.a a
        FROM
          unioned u\
        """,
        query
    );
  }

  @Test
  void nullRightForComparison() {
    equal(ref("someRef"), null);
  }

  @Test
  void unsupportedOperationExceptionForComparisonNullRight() {
    assertThrows(UnsupportedOperationException.class, () -> {
      // Given
      QueryFactory qf = new QueryFactory();

      // When
      qf.build(
          new SelectQueryBuilder(
              ).select(
              false,
              projection("t1", ref("a.a"))
          ).from(
              table("someTable")
          ).where(
              equal(ref("someRef"), null)
          )
      );

      // Then
      // Throw exception
    });

    // Then
    // Throw exception
  }
}
