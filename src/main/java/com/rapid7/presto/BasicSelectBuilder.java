// ===========================================================================
// COPYRIGHT (C) 2019, Rapid7 LLC, Boston, MA, USA.
// ---------------------------------------------------------------------------
// This code is licensed under MIT license (see LICENSE for details)
// ===========================================================================
package com.rapid7.presto;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.Union;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;

class BasicSelectBuilder implements SelectBuilder {
  private final List<Relation> queries = new ArrayList<>();

  private boolean isDistinctProjections;
  private List<ProjectionBuilder> projections;
  private RelationBuilder relation;
  private List<JoinItem> joins = new ArrayList<>();
  private ExpressionBuilder where;
  private boolean isDistinctGroups;
  private List<GroupBuilder> groups;
  private ExpressionBuilder having;
  private SortBuilder[] sorts;
  private String limit;

  @Override
  public BasicSelectBuilder union() {
    completeQuery();

    return this;
  }

  @Override
  public SelectBuilder projection(ProjectionBuilder projection) {
    if (isNull(this.projections)) {
      this.projections = new ArrayList<>();
    }

    this.projections.add(projection);

    return this;
  }

  @Override
  public SelectBuilder conditionalProjection(boolean condition, ProjectionBuilder projection) {
    return condition ? projection(projection) : this;
  }

  @Override
  public BasicSelectBuilder select(boolean isDistinct, ProjectionBuilder... projections) {
    this.isDistinctProjections = isDistinct;
    this.projections = nonNull(projections) ? Stream.of(projections).collect(toList()) : null;

    return this;
  }

  @Override
  public SelectBuilder conditionalSelect(
      boolean condition, boolean isDistinct, ProjectionBuilder... projections
  ) {
    return condition ? select(isDistinct, projections) : this;
  }

  @Override
  public BasicSelectBuilder from(RelationBuilder relation) {
    this.relation = relation;

    return this;
  }

  @Override
  public BasicSelectBuilder conditionalFrom(boolean condition, RelationBuilder relation) {
    return condition ? from(relation) : this;
  }

  @Override
  public BasicSelectBuilder join(JoinType type, RelationBuilder right, ExpressionBuilder on) {
    joins.add(new JoinItem(type, right, on));

    return this;
  }

  @Override
  public BasicSelectBuilder conditionalJoin(
      boolean condition,
      JoinType type,
      RelationBuilder right,
      ExpressionBuilder on
  ) {
    return condition ? join(type, right, on) : this;
  }

  @Override
  public BasicSelectBuilder where(ExpressionBuilder expression) {
    this.where = expression;

    return this;
  }

  @Override
  public BasicSelectBuilder conditionalWhere(boolean condition, ExpressionBuilder expression) {
    return condition ? where(expression) : this;
  }

  @Override
  public BasicSelectBuilder and(ExpressionBuilder expression) {
    if (isNull(this.where)) {
      this.where = expression;
    } else {
      this.where = QueryUtils.conjunction(this.where, expression);
    }

    return this;
  }

  @Override
  public BasicSelectBuilder conditionalAnd(boolean condition, ExpressionBuilder expression) {
    return condition ? and(expression) : this;
  }

  @Override
  public BasicSelectBuilder or(ExpressionBuilder expression) {
    if (isNull(this.where)) {
      this.where = expression;
    } else {
      this.where = QueryUtils.union(this.where, expression);
    }

    return this;
  }

  @Override
  public BasicSelectBuilder conditionalOr(boolean condition, ExpressionBuilder expression) {
    return condition ? or(expression) : this;
  }

  @Override
  public BasicSelectBuilder groupBy(boolean isDistinct, GroupBuilder... groups) {
    this.isDistinctGroups = isDistinct;
    this.groups = nonNull(groups) ? Stream.of(groups).collect(toList()) : null;

    return this;
  }

  @Override
  public BasicSelectBuilder conditionalGroupBy(
      boolean condition,
      boolean isDistinct,
      GroupBuilder... groups
  ) {
    return condition ? groupBy(isDistinct, groups) : this;
  }

  @Override
  public SelectBuilder group(GroupBuilder group) {
    if (isNull(groups)){
      groups = new ArrayList<>();
    }

    groups.add(group);

    return this;
  }

  @Override
  public SelectBuilder conditionalGroup(boolean condition, GroupBuilder group) {
    return condition ? group(group) : this;
  }

  @Override
  public BasicSelectBuilder having(ExpressionBuilder having) {
    this.having = having;

    return this;
  }

  @Override
  public BasicSelectBuilder orderBy(SortBuilder... sorts) {
    this.sorts = sorts;

    return this;
  }

  @Override
  public BasicSelectBuilder limit(String limit) {
    this.limit = limit;

    return this;
  }

  private void completeQuery() {
    if (isNull(projections)) {
      throw new UnsupportedOperationException("You must specify a select to complete this query");
    }

    Select select = new Select(
        isDistinctProjections,
        projections.stream().map(ProjectionBuilder::build).collect(toList())
    );

    Optional<Relation> from = Optional.empty();
    if (null != relation) {
      from = Optional.of(relation.build());
    }

    if (!joins.isEmpty() && !from.isPresent()) {
      throw new UnsupportedOperationException("You cannot join before specifying a from.");
    }

    for (JoinItem joinItem : joins) {
      from = Optional.of(
          new Join(
              joinItem.type.getJoinType(),
              from.get(),
              joinItem.right.build(),
              Optional.of(new JoinOn(joinItem.on.build()))
          )
      );
    }

    Expression whereExpression = null;
    if (null != where) {
      whereExpression = where.build();
    }

    Optional<GroupBy> groupBy = Optional.empty();
    if (null != groups) {
      groupBy = Optional.of(
          new GroupBy(
              isDistinctGroups,
              groups.stream().map(GroupBuilder::build).collect(toList())
          )
      );
    }

    Optional<Expression> havingExpression = Optional.empty();
    if (null != having) {
      havingExpression = Optional.of(having.build());
    }

    Optional<OrderBy> orderBy = Optional.empty();
    if (null != sorts) {
      orderBy = Optional.of(new OrderBy(Stream.of(sorts).map(SortBuilder::build).collect(toList())));
    }

    Optional<String> limitString = Optional.empty();
    if (null != limit) {
      limitString = Optional.of(limit);
    }

    queries.add(
        new QuerySpecification(
            select,
            from,
            Optional.ofNullable(whereExpression),
            groupBy,
            havingExpression,
            orderBy,
            limitString
        )
    );

    isDistinctProjections = false;
    projections = null;
    relation = null;
    joins = new ArrayList<>();
    where = null;
    isDistinctGroups = false;
    groups = null;
    having = null;
    sorts = null;
    limit = null;
  }

  public QueryBody build() {
    completeQuery();

    if (queries.size() > 1) {
      return new Union(queries, false);
    } else {
      return (QueryBody) queries.get(0);
    }
  }

  private static class JoinItem {
    private JoinType type;
    private RelationBuilder right;
    private ExpressionBuilder on;

    private JoinItem(JoinType type, RelationBuilder right, ExpressionBuilder on) {
      this.type = type;
      this.right = right;
      this.on = on;
    }
  }
}
