package com.rapid7.presto;

import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import java.util.Objects;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;

public class GroupBuilder {
  private ExpressionBuilder expression;

  GroupBuilder(ExpressionBuilder expression) {
    this.expression = expression;
  }

  GroupingElement build() {
    return new SimpleGroupBy(Stream.of(expression).map(ExpressionBuilder::build).collect(toList()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GroupBuilder that = (GroupBuilder) o;
    return expression.equals(that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression);
  }
}
