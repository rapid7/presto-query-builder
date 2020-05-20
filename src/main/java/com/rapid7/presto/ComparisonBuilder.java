package com.rapid7.presto;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import static java.util.stream.Collectors.toList;

public class ComparisonBuilder extends ExpressionBuilder {
  private ComparisonExpression.Operator comparisonOperator;
  private ExpressionBuilder left;
  private ExpressionBuilder right;

  private LogicalBinaryExpression.Operator logicalOperator;
  private ExpressionBuilder[] expressions;

  ComparisonBuilder(
      ComparisonExpression.Operator comparisonOperator,
      ExpressionBuilder left,
      ExpressionBuilder right
  ) {
    this.comparisonOperator = comparisonOperator;
    this.left = left;
    this.right = right;
  }

  ComparisonBuilder(LogicalBinaryExpression.Operator logicalOperator, ExpressionBuilder... expressions) {
    this.logicalOperator = logicalOperator;
    this.expressions = expressions;
  }

  @Override
  Expression build() {
    if (null != logicalOperator) {
      if (null == expressions) {
        throw new UnsupportedOperationException("There must be at least 1 expression.");
      }

      return chainLogicalBinaryExpressions(
          logicalOperator,
          Arrays.stream(expressions).collect(toList())
      );
    } else {
      if (null == left || null == right) {
        throw new UnsupportedOperationException("Both sides of the comparison expression must be defined.");
      }

      return new ComparisonExpression(comparisonOperator, left.build(), right.build());
    }
  }

  private Expression chainLogicalBinaryExpressions(
      LogicalBinaryExpression.Operator operator,
      List<ExpressionBuilder> expressions
  ) {
    Iterator<Expression> it = expressions.stream().filter(Objects::nonNull).map(ExpressionBuilder::build).iterator();
    if (!it.hasNext()) {
      throw new UnsupportedOperationException("There must be at least 1 expression.");
    }

    Expression expression = it.next();
    while (it.hasNext()) {
      expression = new LogicalBinaryExpression(operator, expression, it.next());
    }

    return expression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ComparisonBuilder that = (ComparisonBuilder) o;
    return comparisonOperator == that.comparisonOperator &&
        Objects.equals(left, that.left) &&
        Objects.equals(right, that.right) &&
        logicalOperator == that.logicalOperator &&
        Arrays.equals(expressions, that.expressions);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(comparisonOperator, left, right, logicalOperator);
    result = 31 * result + Arrays.hashCode(expressions);
    return result;
  }
}
