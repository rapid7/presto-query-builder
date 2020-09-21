// ===========================================================================
// COPYRIGHT (C) 2019, Rapid7 LLC, Boston, MA, USA.
// ---------------------------------------------------------------------------
// This code is licensed under MIT license (see LICENSE for details)
// ===========================================================================
package com.rapid7.presto;

import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.With;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static java.util.stream.Collectors.toList;

public class SelectQueryBuilder implements QueryBuilder, SelectBuilder {
  private final List<WithSelectBuilder> withQueries = new ArrayList<>();
  private final BasicSelectBuilder basicSelectBuilder;

  public SelectQueryBuilder() {
    this.basicSelectBuilder = new BasicSelectBuilder();
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<With> with = Optional.empty();

  public WithSelectBuilder with(String name) {
    WithSelectBuilder withSelectBuilder = new WithSelectBuilder(this, name);
    withQueries.add(withSelectBuilder);

    return withSelectBuilder;
  }

  @Override
  public SelectQueryBuilder union() {
    basicSelectBuilder.union();

    return this;
  }

  @Override
  public SelectQueryBuilder select(boolean isDistinct, ProjectionBuilder... projections) {
    basicSelectBuilder.select(isDistinct, projections);

    return this;
  }

  @Override
  public SelectQueryBuilder conditionalSelect(boolean condition, boolean isDistinct, ProjectionBuilder... projections) {
    basicSelectBuilder.conditionalSelect(condition, isDistinct, projections);

    return this;
  }

  @Override
  public SelectQueryBuilder projection(ProjectionBuilder projection) {
    basicSelectBuilder.projection(projection);

    return this;
  }

  @Override
  public SelectQueryBuilder conditionalProjection(boolean condition, ProjectionBuilder projection) {
    basicSelectBuilder.conditionalProjection(condition, projection);

    return this;
  }

  @Override
  public SelectQueryBuilder from(RelationBuilder from) {
    basicSelectBuilder.from(from);

    return this;
  }

  @Override
  public SelectQueryBuilder conditionalFrom(boolean condition, RelationBuilder from) {
    basicSelectBuilder.conditionalFrom(condition, from);

    return this;
  }

  @Override
  public SelectQueryBuilder join(JoinType type, RelationBuilder right, ExpressionBuilder on) {
    basicSelectBuilder.join(type, right, on);

    return this;
  }

  @Override
  public SelectQueryBuilder conditionalJoin(
      boolean condition,
      JoinType type,
      RelationBuilder right,
      ExpressionBuilder on
  ) {
    basicSelectBuilder.conditionalJoin(condition, type, right, on);

    return this;
  }

  @Override
  public SelectQueryBuilder where(ExpressionBuilder expression) {
    basicSelectBuilder.where(expression);

    return this;
  }

  @Override
  public SelectQueryBuilder conditionalWhere(boolean condition, ExpressionBuilder expression) {
    basicSelectBuilder.conditionalWhere(condition, expression);

    return this;
  }

  @Override
  public SelectQueryBuilder and(ExpressionBuilder expression) {
    basicSelectBuilder.and(expression);

    return this;
  }

  @Override
  public SelectQueryBuilder conditionalAnd(boolean condition, ExpressionBuilder expression) {
    basicSelectBuilder.conditionalAnd(condition, expression);

    return this;
  }

  @Override
  public SelectQueryBuilder or(ExpressionBuilder expression) {
    basicSelectBuilder.or(expression);

    return this;
  }

  @Override
  public SelectQueryBuilder conditionalOr(boolean condition, ExpressionBuilder expression) {
    basicSelectBuilder.conditionalOr(condition, expression);

    return this;
  }

  @Override
  public SelectQueryBuilder groupBy(boolean isDistinct, GroupBuilder... groups) {
    basicSelectBuilder.groupBy(isDistinct, groups);

    return this;
  }

  @Override
  public SelectQueryBuilder conditionalGroupBy(
      boolean condition,
      boolean isDistinct,
      GroupBuilder... groups
  ) {
    basicSelectBuilder.conditionalGroupBy(condition, isDistinct, groups);

    return this;
  }

  @Override
  public SelectQueryBuilder group(GroupBuilder group) {
    basicSelectBuilder.group(group);

    return this;
  }

  @Override
  public SelectQueryBuilder conditionalGroup(boolean condition, GroupBuilder group) {
    basicSelectBuilder.conditionalGroup(condition, group);

    return this;
  }

  @Override
  public SelectQueryBuilder having(ExpressionBuilder having) {
    basicSelectBuilder.having(having);

    return this;
  }

  @Override
  public SelectQueryBuilder orderBy(SortBuilder... sorts) {
    basicSelectBuilder.orderBy(sorts);

    return this;
  }

  @Override
  public SelectQueryBuilder limit(String limit) {
    basicSelectBuilder.limit(limit);

    return this;
  }

  public Statement build() {
    if (!withQueries.isEmpty()) {
      with = Optional.of(new With(false, withQueries.stream().map(WithSelectBuilder::build).collect(toList())));
    }

    return new Query(with, basicSelectBuilder.build(), Optional.empty(), Optional.empty());
  }
}
