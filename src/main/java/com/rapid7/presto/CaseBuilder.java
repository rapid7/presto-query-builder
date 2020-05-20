package com.rapid7.presto;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;

public class CaseBuilder extends ExpressionBuilder {
  private ExpressionBuilder operand;
  private ExpressionBuilder fallback;
  private WhenBuilder[] whens;

  CaseBuilder(ExpressionBuilder operand, ExpressionBuilder fallback, WhenBuilder... whens) {
    this.operand = operand;
    this.fallback = fallback;
    this.whens = whens;
  }

  @Override
  Expression build() {
    if (null != operand) {
      return new SimpleCaseExpression(
          operand.build(),                                                    // condition
          Stream.of(whens).map(WhenBuilder::build).collect(toList()),         // values
          null != fallback ? Optional.of(fallback.build()) : Optional.empty() // else
      );
    } else {
      return new SearchedCaseExpression(
          Stream.of(whens).map(WhenBuilder::build).collect(toList()),         // ifs and else ifs
          null != fallback ? Optional.of(fallback.build()) : Optional.empty() // else
      );
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CaseBuilder that = (CaseBuilder) o;
    return Objects.equals(operand, that.operand) &&
        fallback.equals(that.fallback) &&
        Arrays.equals(whens, that.whens);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(operand, fallback);
    result = 31 * result + Arrays.hashCode(whens);
    return result;
  }
}
