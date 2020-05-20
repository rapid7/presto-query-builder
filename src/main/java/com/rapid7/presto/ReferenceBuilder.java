package com.rapid7.presto;

import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import java.util.Objects;

public class ReferenceBuilder extends ExpressionBuilder {
  private String searchPath;
  private String reference;

  ReferenceBuilder(String reference) {
    this.searchPath = null;
    this.reference = reference;
  }

  ReferenceBuilder(String searchPath, String reference) {
    this.searchPath = searchPath;
    this.reference = reference;
  }

  @Override
  Expression build() {
    String ref = reference;
    if (null != searchPath) {
      ref = searchPath + "." + ref;
    }

    Expression finalExpression = null;

    String[] parts = ref.split("\\.");
    int cur = 0;
    while (cur < parts.length) {
      if (cur == 0 ) {
        finalExpression = new Identifier(parts[cur]);
      } else {
        finalExpression = new DereferenceExpression(finalExpression, new Identifier(parts[cur]));
      }
      cur++;
    }

    return finalExpression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReferenceBuilder that = (ReferenceBuilder) o;
    return Objects.equals(searchPath, that.searchPath) &&
        reference.equals(that.reference);
  }

  @Override
  public int hashCode() {
    return Objects.hash(searchPath, reference);
  }
}
