package com.rapid7.presto;

import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.Expression;
import java.util.Objects;

public class BetweenBuilder extends ExpressionBuilder {
  private ExpressionBuilder value;
  private ExpressionBuilder min;
  private ExpressionBuilder max;

  BetweenBuilder(
      ExpressionBuilder value,
      ExpressionBuilder min,
      ExpressionBuilder max
  ) {
    this.value = value;
    this.min = min;
    this.max = max;
  }

  @Override
  Expression build() {
    return new BetweenPredicate(value.build(), min.build(), max.build());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BetweenBuilder that = (BetweenBuilder) o;
    return value.equals(that.value) &&
        min.equals(that.min) &&
        max.equals(that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, min, max);
  }
}
