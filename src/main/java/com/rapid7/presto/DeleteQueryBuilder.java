// ===========================================================================
// COPYRIGHT (C) 2019, Rapid7 LLC, Boston, MA, USA.
// ---------------------------------------------------------------------------
// This code is licensed under MIT license (see LICENSE for details)
// ===========================================================================
package com.rapid7.presto;

import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import java.util.Optional;

public class DeleteQueryBuilder implements QueryBuilder {
  private Table table = null;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<Expression> where = Optional.empty();

  public DeleteQueryBuilder from(Table table) {
    this.table = table;

    return this;
  }

  public DeleteQueryBuilder where(Expression expression) {
    this.where = Optional.of(expression);

    return this;
  }

  public Statement build() {
    if (null == table) {
      throw new UnsupportedOperationException("You must specify a table to complete this query");
    }

    return new Delete(new NodeLocation(1, 0), table, where);
  }
}
