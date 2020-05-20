package com.rapid7.presto;

import com.facebook.presto.sql.tree.SortItem;
import java.util.Objects;

public class SortBuilder {
  private ExpressionBuilder dereferenceExpression;
  private SortOrdering ordering;
  private SortNullOrdering nullOrdering;

  SortBuilder(
      ExpressionBuilder dereferenceExpression,
      SortOrdering ordering,
      SortNullOrdering nullOrdering
  ) {
    this.dereferenceExpression = dereferenceExpression;
    this.ordering = ordering;
    this.nullOrdering = nullOrdering;
  }

  SortItem build() {
    return new SortItem(dereferenceExpression.build(), ordering.getOrdering(), nullOrdering.getNullOrdering());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SortBuilder that = (SortBuilder) o;
    return dereferenceExpression.equals(that.dereferenceExpression) &&
        ordering == that.ordering &&
        nullOrdering == that.nullOrdering;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dereferenceExpression, ordering, nullOrdering);
  }
}
