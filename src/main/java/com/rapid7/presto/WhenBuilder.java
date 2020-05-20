package com.rapid7.presto;

import com.facebook.presto.sql.tree.WhenClause;
import java.util.Objects;

public class WhenBuilder {
  private ExpressionBuilder operand;
  private ExpressionBuilder result;

  WhenBuilder(ExpressionBuilder operand, ExpressionBuilder result) {
    this.operand = operand;
    this.result = result;
  }

  WhenClause build() {
    return new WhenClause(operand.build(), result.build());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WhenBuilder that = (WhenBuilder) o;
    return operand.equals(that.operand) &&
        result.equals(that.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operand, result);
  }
}
