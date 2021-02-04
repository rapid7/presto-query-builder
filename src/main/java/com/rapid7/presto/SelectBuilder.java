package com.rapid7.presto;

import java.util.function.Function;

public interface SelectBuilder {
  SelectBuilder union();
  SelectBuilder select(boolean isDistinct, ProjectionBuilder... projections);
  SelectBuilder conditionalSelect(boolean condition, boolean isDistinct, ProjectionBuilder... projections);
  SelectBuilder projection(ProjectionBuilder projection);
  SelectBuilder conditionalProjection(boolean condition, ProjectionBuilder projection);
  SelectBuilder from(RelationBuilder from);
  SelectBuilder conditionalFrom(boolean condition, RelationBuilder from);
  SelectBuilder join(JoinType type, RelationBuilder right, ExpressionBuilder on);
  SelectBuilder conditionalJoin(boolean condition, JoinType type, RelationBuilder right, ExpressionBuilder on);
  SelectBuilder where(ExpressionBuilder expression);
  SelectBuilder conditionalWhere(boolean condition, ExpressionBuilder expression);
  SelectBuilder and(ExpressionBuilder expression);
  SelectBuilder conditionalAnd(boolean condition, ExpressionBuilder expression);
  SelectBuilder or(ExpressionBuilder expression);
  SelectBuilder conditionalOr(boolean condition, ExpressionBuilder expression);
  SelectBuilder groupBy(boolean isDistinct, GroupBuilder... groups);
  SelectBuilder group(GroupBuilder group);
  SelectBuilder conditionalGroup(boolean condition, GroupBuilder group);
  SelectBuilder conditionalGroupBy(boolean condition, boolean isDistinct, GroupBuilder... groups);
  SelectBuilder having(ExpressionBuilder having);
  SelectBuilder orderBy(SortBuilder... sorts);
  SelectBuilder limit(String limit);
  SelectBuilder chain(Function<SelectBuilder, Void> chain);
}
