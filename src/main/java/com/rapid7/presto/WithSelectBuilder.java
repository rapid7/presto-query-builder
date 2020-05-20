// ===========================================================================
// COPYRIGHT (C) 2019, Rapid7 LLC, Boston, MA, USA.
// ---------------------------------------------------------------------------
// This code is licensed under MIT license (see LICENSE for details)
// ===========================================================================
package com.rapid7.presto;

import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.WithQuery;
import java.util.Optional;

public class WithSelectBuilder implements SelectBuilder {
  private SelectQueryBuilder selectQueryBuilder;
  private String name;
  private BasicSelectBuilder basicSelectBuilder;

  public WithSelectBuilder(SelectQueryBuilder selectQueryBuilder, String name) {
    this.selectQueryBuilder = selectQueryBuilder;
    this.name = name;
    this.basicSelectBuilder = new BasicSelectBuilder();
  }

  @Override
  public WithSelectBuilder union() {
    basicSelectBuilder.union();

    return this;
  }

  @Override
  public WithSelectBuilder select(boolean isDistinct, ProjectionBuilder... selectItems) {
    basicSelectBuilder.select(isDistinct, selectItems);

    return this;
  }

  @Override
  public WithSelectBuilder from(RelationBuilder from) {
    basicSelectBuilder.from(from);

    return this;
  }

  @Override
  public WithSelectBuilder conditionalFrom(boolean condition, RelationBuilder from) {
    basicSelectBuilder.conditionalFrom(condition, from);

    return this;
  }

  @Override
  public WithSelectBuilder join(JoinType type, RelationBuilder right, ExpressionBuilder on) {
    basicSelectBuilder.join(type, right, on);

    return this;
  }

  @Override
  public WithSelectBuilder conditionalJoin(
      boolean condition,
      JoinType type,
      RelationBuilder right,
      ExpressionBuilder on
  ) {
    basicSelectBuilder.conditionalJoin(condition, type, right, on);

    return this;
  }

  @Override
  public WithSelectBuilder where(ExpressionBuilder expression) {
    basicSelectBuilder.where(expression);

    return this;
  }

  @Override
  public WithSelectBuilder conditionalWhere(boolean condition, ExpressionBuilder expression) {
    basicSelectBuilder.conditionalWhere(condition, expression);

    return this;
  }

  @Override
  public WithSelectBuilder and(ExpressionBuilder expression) {
    basicSelectBuilder.and(expression);

    return this;
  }

  @Override
  public WithSelectBuilder conditionalAnd(boolean condition, ExpressionBuilder expression) {
    basicSelectBuilder.conditionalAnd(condition, expression);

    return this;
  }

  @Override
  public WithSelectBuilder or(ExpressionBuilder expression) {
    basicSelectBuilder.or(expression);

    return this;
  }

  @Override
  public WithSelectBuilder conditionalOr(boolean condition, ExpressionBuilder expression) {
    basicSelectBuilder.conditionalOr(condition, expression);

    return this;
  }

  @Override
  public WithSelectBuilder groupBy(boolean isDistinct, GroupBuilder... groups) {
    basicSelectBuilder.groupBy(isDistinct, groups);

    return this;
  }

  @Override
  public WithSelectBuilder conditionalGroupBy(
      boolean condition,
      boolean isDistinct,
      GroupBuilder... groups
  ) {
    basicSelectBuilder.conditionalGroupBy(condition, isDistinct, groups);

    return this;
  }

  @Override
  public WithSelectBuilder having(ExpressionBuilder having) {
    basicSelectBuilder.having(having);

    return this;
  }

  @Override
  public WithSelectBuilder orderBy(SortBuilder... sorts) {
    basicSelectBuilder.orderBy(sorts);

    return this;
  }

  @Override
  public WithSelectBuilder limit(String limit) {
    basicSelectBuilder.limit(limit);

    return this;
  }

  public SelectQueryBuilder end() {
    return selectQueryBuilder;
  }

  public WithQuery build() {
    return new WithQuery(
        new Identifier(name),
        new Query(
            Optional.empty(),
            basicSelectBuilder.build(),
            Optional.empty(),
            Optional.empty()
        ),
        Optional.empty()
    );
  }
}
