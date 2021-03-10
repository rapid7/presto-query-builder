package com.rapid7.presto;

import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;

public class CastBuilder extends ExpressionBuilder {
  private final String type;
  private final ExpressionBuilder expression;

  public CastBuilder(String type, ExpressionBuilder expression) {
    this.type = type;
    this.expression = expression;
  }

  @Override
  Expression build() {
    return new Cast(expression.build(), type);
  }
}
