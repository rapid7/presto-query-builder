package com.rapid7.presto;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SubqueryExpression;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;

public class InBuilder extends ExpressionBuilder {
  private boolean isNot;
  private ExpressionBuilder left;
  private ExpressionBuilder[] literals;
  private ProjectionBuilder projection;
  private RelationBuilder relation;
  private ExpressionBuilder where;

  InBuilder(boolean isNot, ExpressionBuilder left, ExpressionBuilder[] literals) {
    this.isNot = isNot;
    this.left = left;
    this.literals = literals;
    this.projection = null;
    this.relation = null;
    this.where = null;
  }

  InBuilder(
      boolean isNot,
      ExpressionBuilder left,
      ProjectionBuilder projection,
      RelationBuilder relation,
      ExpressionBuilder where
  ) {
    this.isNot = isNot;
    this.left = left;
    this.literals = null;
    this.projection = projection;
    this.relation = relation;
    this.where = where;
  }

  @Override
  Expression build() {
    Expression expression;
    if (null != projection) {
      expression = new InPredicate(
          left.build(),
          new SubqueryExpression(
              new Query(
                  Optional.empty(),
                  new QuerySpecification(
                      new Select(false, Stream.of(projection.build()).collect(toList())),
                      Optional.of(relation.build()),
                      null != where ? Optional.of(where.build()) : Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty()
                  ),
                  Optional.empty(),
                  Optional.empty()
              )
          )
      );
    } else {
      expression = new InPredicate(
          left.build(),
          new InListExpression(Arrays.stream(literals).map(ExpressionBuilder::build).collect(toList()))
      );
    }

    if (isNot) {
      expression = new NotExpression(expression);
    }

    return expression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InBuilder inBuilder = (InBuilder) o;
    return isNot == inBuilder.isNot &&
        left.equals(inBuilder.left) &&
        Arrays.equals(literals, inBuilder.literals) &&
        Objects.equals(projection, inBuilder.projection) &&
        Objects.equals(relation, inBuilder.relation) &&
        Objects.equals(where, inBuilder.where);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(isNot, left, projection, relation, where);
    result = 31 * result + Arrays.hashCode(literals);
    return result;
  }
}
