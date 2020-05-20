package com.rapid7.presto;

import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.Expression;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import static java.util.stream.Collectors.toList;

public class ArithmeticsBuilder extends ExpressionBuilder {
  private ArithmeticBinaryExpression.Operator operator;
  private ExpressionBuilder[] expressions;

  ArithmeticsBuilder(ArithmeticBinaryExpression.Operator operator, ExpressionBuilder... expressions) {
    this.operator = operator;
    this.expressions = expressions;
  }

  @Override
  Expression build() {
    if (null == expressions) {
      throw new UnsupportedOperationException("There must be at least 1 expression.");
    }

    return chainArithmeticBinaryExpressions(
        operator,
        Arrays.stream(expressions).collect(toList())
    );
  }

  private Expression chainArithmeticBinaryExpressions(
      ArithmeticBinaryExpression.Operator operator,
      List<ExpressionBuilder> expressions
  ) {
    Iterator<Expression> it = expressions.stream().filter(Objects::nonNull).map(ExpressionBuilder::build).iterator();
    if (!it.hasNext()) {
      throw new UnsupportedOperationException("There must be at least 1 expression.");
    }

    Expression expression = it.next();
    while (it.hasNext()) {
      expression = new ArithmeticBinaryExpression(operator, expression, it.next());
    }

    return expression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArithmeticsBuilder that = (ArithmeticsBuilder) o;
    return operator == that.operator &&
        Arrays.equals(expressions, that.expressions);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(operator);
    result = 31 * result + Arrays.hashCode(expressions);
    return result;
  }
}
