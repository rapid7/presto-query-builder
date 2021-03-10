package com.rapid7.presto;

import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.MODULUS;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.SUBTRACT;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.AND;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.OR;

public class QueryUtils {
  // Literals
  public static ExpressionBuilder lit(Object obj) {
    return new LiteralBuilder(obj);
  }

  public static ExpressionBuilder intervalLit(String value, IntervalSign sign, IntervalField field) {
    return new LiteralBuilder(value, sign, field);
  }

  // Reference Expressions
  public static ExpressionBuilder param(QueryFactory queryFactory, String key) {
    return new ParameterBuilder(queryFactory.getParameterMap(), key);
  }

  public static QualifiedNameBuilder qualifiedName(String catalog, String schema, String fieldName) {
    return new QualifiedNameBuilder(catalog, schema, fieldName);
  }

  public static ExpressionBuilder ref(String searchPath, String reference) {
    return new ReferenceBuilder(searchPath, reference);
  }

  public static ExpressionBuilder ref(String reference) {
    return new ReferenceBuilder(null, reference);
  }

  public static ExpressionBuilder cast(String type, ExpressionBuilder expression) {
    return new CastBuilder(type, expression);
  }

  // Function Expressions
  public static ExpressionBuilder arbitrary(ExpressionBuilder expression) {
    return new FunctionBuilder("arbitrary", false, expression);
  }

  public static ExpressionBuilder coalesce(ExpressionBuilder... expressions) {
    return new FunctionBuilder("coalesce", false, expressions);
  }

  public static ExpressionBuilder sum(ExpressionBuilder expression) {
    return new FunctionBuilder("sum", false, expression);
  }

  public static ExpressionBuilder avg(ExpressionBuilder expression) {
    return new FunctionBuilder("avg", false, expression);
  }

  public static ExpressionBuilder min(ExpressionBuilder expression) {
    return new FunctionBuilder("min", false, expression);
  }

  public static ExpressionBuilder max(ExpressionBuilder expression) {
    return new FunctionBuilder("max", false, expression);
  }

  public static ExpressionBuilder count(ExpressionBuilder expression) {
    return new FunctionBuilder("count", false, expression);
  }

  public static ExpressionBuilder count(boolean isDistinct, ExpressionBuilder expression) {
    return new FunctionBuilder("count", isDistinct, expression);
  }

  public static ExpressionBuilder countIf(ExpressionBuilder expression) {
    return new FunctionBuilder("count_if", false, expression);
  }

  public static ExpressionBuilder countIf(boolean isDistinct, ExpressionBuilder expression) {
    return new FunctionBuilder("count_if", isDistinct, expression);
  }

  public static ExpressionBuilder dateDiff(String unit, ExpressionBuilder leftExpression, ExpressionBuilder rightExpression) {
    return new FunctionBuilder("date_diff", false, lit(unit), rightExpression, leftExpression);
  }

  public static ExpressionBuilder fromIso8601Date(ExpressionBuilder expression) {
    return new FunctionBuilder("from_iso8601_date", false, expression);
  }

  public static ExpressionBuilder fromIso8601Timestamp(ExpressionBuilder expression) {
    return new FunctionBuilder("from_iso8601_timestamp", false, expression);
  }

  public static ExpressionBuilder toIso8601(ExpressionBuilder expression) {
    return new FunctionBuilder("to_iso8601", false, expression);
  }

  public static ExpressionBuilder fromUnixTime(ExpressionBuilder expression) {
    return new FunctionBuilder("from_unixtime", false, expression);
  }

  public static ExpressionBuilder toUnixTime(ExpressionBuilder expression) {
    return new FunctionBuilder("to_unixtime", false, expression);
  }

  // Operator Expressions
  public static ExpressionBuilder inSubquery(
      ExpressionBuilder left,
      ProjectionBuilder projection,
      RelationBuilder relation
  ) {
    return new InBuilder(false, left, projection, relation, null);
  }

  public static ExpressionBuilder notInSubquery(
      ExpressionBuilder left,
      ProjectionBuilder projection,
      RelationBuilder relation
  ) {
    return new InBuilder(true, left, projection, relation, null);
  }

  public static ExpressionBuilder inSubquery(
      ExpressionBuilder left,
      ProjectionBuilder projection,
      RelationBuilder relation,
      ExpressionBuilder where
  ) {
    return new InBuilder(false, left, projection, relation, where);
  }

  public static ExpressionBuilder notInSubquery(
      ExpressionBuilder left,
      ProjectionBuilder projection,
      RelationBuilder relation,
      ExpressionBuilder where
  ) {
    return new InBuilder(true, left, projection, relation, where);
  }

  public static ExpressionBuilder in(ExpressionBuilder left, ExpressionBuilder... literals) {
    return new InBuilder(false, left, literals);
  }

  public static ExpressionBuilder notIn(ExpressionBuilder left, ExpressionBuilder... literals) {
    return new InBuilder(true, left, literals);
  }

  public static ExpressionBuilder is(ExpressionBuilder expression) {
    return new IsBuilder(false, expression);
  }

  public static ExpressionBuilder isNot(ExpressionBuilder expression) {
    return new IsBuilder(true, expression);
  }

  public static ExpressionBuilder equal(ExpressionBuilder left, ExpressionBuilder right) {
    return new ComparisonBuilder(EQUAL, left, right);
  }

  public static ExpressionBuilder notEqual(ExpressionBuilder left, ExpressionBuilder right) {
    return new ComparisonBuilder(NOT_EQUAL, left, right);
  }

  public static ExpressionBuilder lessThan(ExpressionBuilder left, ExpressionBuilder right) {
    return new ComparisonBuilder(LESS_THAN, left, right);
  }

  public static ExpressionBuilder lessThanOrEqual(ExpressionBuilder left, ExpressionBuilder right) {
    return new ComparisonBuilder(LESS_THAN_OR_EQUAL, left, right);
  }

  public static ExpressionBuilder greaterThan(ExpressionBuilder left, ExpressionBuilder right) {
    return new ComparisonBuilder(GREATER_THAN, left, right);
  }

  public static ExpressionBuilder greaterThanOrEqual(ExpressionBuilder left, ExpressionBuilder right) {
    return new ComparisonBuilder(GREATER_THAN_OR_EQUAL, left, right);
  }

  public static ExpressionBuilder conjunction(ExpressionBuilder... expressions) {
    return new ComparisonBuilder(AND, expressions);
  }

  public static ExpressionBuilder union(ExpressionBuilder... expressions) {
    return new ComparisonBuilder(OR, expressions);
  }

  public static ExpressionBuilder add(ExpressionBuilder... values) {
    return new ArithmeticsBuilder(ADD, values);
  }

  public static ExpressionBuilder subtract(ExpressionBuilder... values) {
    return new ArithmeticsBuilder(SUBTRACT, values);
  }

  public static ExpressionBuilder multiply(ExpressionBuilder... values) {
    return new ArithmeticsBuilder(MULTIPLY, values);
  }

  public static ExpressionBuilder divide(ExpressionBuilder... values) {
    return new ArithmeticsBuilder(DIVIDE, values);
  }

  public static ExpressionBuilder modulus(ExpressionBuilder... values) {
    return new ArithmeticsBuilder(MODULUS, values);
  }

  public static ExpressionBuilder between(ExpressionBuilder value, ExpressionBuilder min, ExpressionBuilder max) {
    return new BetweenBuilder(value, min, max);
  }

  // Special Expressions
  public static ExpressionBuilder row(ExpressionBuilder... expressions) {
    return new RowBuilder(expressions);
  }

  public static ExpressionBuilder simpleCase(
      ExpressionBuilder operand,
      ExpressionBuilder fallback,
      WhenBuilder... whens
  ) {
    return new CaseBuilder(operand, fallback, whens);
  }

  public static ExpressionBuilder searchedCase(ExpressionBuilder fallback, WhenBuilder... whens) {
    return new CaseBuilder(null, fallback, whens);
  }

  public static WhenBuilder when(ExpressionBuilder operand, ExpressionBuilder result) {
    return new WhenBuilder(operand, result);
  }

  // Projections
  public static ProjectionBuilder projection(String alias, ExpressionBuilder expression) {
    return new ProjectionBuilder(alias, expression);
  }

  // Relations
  public static RelationBuilder table(String name) {
    return new RelationBuilder(name);
  }

  public static RelationBuilder aliasedTable(String alias, String name) {
    return new RelationBuilder(alias, name);
  }

  // Grouping
  public static GroupBuilder group(ExpressionBuilder expression) {
    return new GroupBuilder(expression);
  }

  // Sorting
  public static SortBuilder order(
      ExpressionBuilder expression,
      SortOrdering ordering,
      SortNullOrdering nullOrdering
  ) {
    return new SortBuilder(expression, ordering, nullOrdering);
  }
}
