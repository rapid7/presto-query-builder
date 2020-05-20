package com.rapid7.presto;

import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.SingleColumn;
import java.util.Objects;

public class ProjectionBuilder {
  private String alias;
  private ExpressionBuilder expression;

  ProjectionBuilder(String alias, ExpressionBuilder expression) {
    this.alias = alias;
    this.expression = expression;
  }

  SingleColumn build() {
    return new SingleColumn(expression.build(), new Identifier(alias));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ProjectionBuilder that = (ProjectionBuilder) o;
    return alias.equals(that.alias) &&
        expression.equals(that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, expression);
  }
}
