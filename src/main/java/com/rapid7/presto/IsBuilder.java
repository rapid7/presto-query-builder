package com.rapid7.presto;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import java.util.Objects;

public class IsBuilder extends ExpressionBuilder {
  private boolean isNot;
  private ExpressionBuilder expression;

  IsBuilder(boolean isNot, ExpressionBuilder expression) {
    this.isNot = isNot;
    this.expression = expression;
  }

  @Override
  Expression build() {
    if (isNot) {
      return new IsNotNullPredicate(expression.build());
    } else {
      return new IsNullPredicate(expression.build());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IsBuilder isBuilder = (IsBuilder) o;
    return isNot == isBuilder.isNot &&
        expression.equals(isBuilder.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isNot, expression);
  }
}
