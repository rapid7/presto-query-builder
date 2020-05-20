// ===========================================================================
// COPYRIGHT (C) 2019, Rapid7 LLC, Boston, MA, USA.
// ---------------------------------------------------------------------------
// This code is licensed under MIT license (see LICENSE for details)
// ===========================================================================
package com.rapid7.presto;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import static java.util.stream.Collectors.toList;

public class InsertQueryBuilder implements QueryBuilder {
  private QualifiedNameBuilder target = null;
  private List<Identifier> columns = new ArrayList<>();
  private List<Expression> rows = new ArrayList<>();
  private Map<Identifier, ExpressionBuilder> row = new HashMap<>();

  public InsertQueryBuilder into(QualifiedNameBuilder target, List<ExpressionBuilder> columns) {
    this.target = target;
    this.columns = new ArrayList<>();
    for (ExpressionBuilder column : columns) {
      Expression expression = column.build();
      if (!(expression instanceof Identifier)) {
        throw new UnsupportedOperationException("The identifier must be a reference.");
      }

      this.columns.add((Identifier) expression);
    }

    return this;
  }

  public InsertQueryBuilder row() {
    this.row.clear();

    return this;
  }

  public InsertQueryBuilder value(ExpressionBuilder identifier, ExpressionBuilder expression) {
    Expression id = identifier.build();
    if (!(id instanceof Identifier)) {
      throw new UnsupportedOperationException("The identifier must be a reference.");
    }

    this.row.put((Identifier) id, expression);

    return this;
  }

  public InsertQueryBuilder end() {
    if (row.isEmpty()) {
      throw new UnsupportedOperationException("You must specify some rows to complete this query.");
    }

    int numUnspecified = columns.size();
    Set<Identifier> keys = row.keySet();
    for (Identifier column : columns) {
      if (keys.contains(column)) {
        numUnspecified--;
      }
    }

    if (numUnspecified > 0) {
      throw new UnsupportedOperationException("You must specify a value for each column.");
    }

    List<ExpressionBuilder> orderedValues = new ArrayList<>();
    for (Identifier column : columns) {
      orderedValues.add(row.get(column));
    }

    this.rows.add(new Row(orderedValues.stream().map(ExpressionBuilder::build).collect(toList())));

    return this;
  }

  public Statement build() {
    if (null == target) {
      throw new UnsupportedOperationException("You must specify a target to complete this query.");
    }

    if (rows.isEmpty()) {
      throw new UnsupportedOperationException("You must specify some rows to complete this query.");
    }

    return new Insert(
        target.build(),
        Optional.of(columns),
        new Query(
            Optional.empty(),
            new Values(rows),
            Optional.empty(),
            Optional.empty()
        )
    );
  }
}
