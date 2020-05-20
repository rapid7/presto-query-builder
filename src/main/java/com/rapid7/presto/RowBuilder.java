package com.rapid7.presto;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Row;
import java.util.Arrays;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;

public class RowBuilder extends ExpressionBuilder {
  private ExpressionBuilder[] expressions;

  RowBuilder(ExpressionBuilder... expressions) {
    this.expressions = expressions;
  }

  @Override
  Expression build() {
    return new Row(Stream.of(expressions).map(ExpressionBuilder::build).collect(toList()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RowBuilder that = (RowBuilder) o;
    return Arrays.equals(expressions, that.expressions);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(expressions);
  }
}
