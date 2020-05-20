package com.rapid7.presto;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;

public class FunctionBuilder extends ExpressionBuilder {
  private String name;
  private boolean isDistinct;
  private ExpressionBuilder[] expressions;

  FunctionBuilder(
      String name,
      boolean isDistinct,
      ExpressionBuilder... expressions
  ) {
    this.name = name;
    this.isDistinct = isDistinct;
    this.expressions = expressions;
  }

  @Override
  Expression build() {
    return new FunctionCall(
        QualifiedName.of(name),
        isDistinct,
        Stream.of(expressions).map(ExpressionBuilder::build).collect(toList())
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FunctionBuilder that = (FunctionBuilder) o;
    return isDistinct == that.isDistinct &&
        name.equals(that.name) &&
        Arrays.equals(expressions, that.expressions);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name, isDistinct);
    result = 31 * result + Arrays.hashCode(expressions);
    return result;
  }
}
