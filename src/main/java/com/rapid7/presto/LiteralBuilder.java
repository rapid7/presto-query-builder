package com.rapid7.presto;

import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import java.sql.Timestamp;
import java.util.Objects;

public class LiteralBuilder extends ExpressionBuilder {
  private Object literal;
  private String intervalValue;
  private IntervalSign intervalSign;
  private IntervalField intervalField;

  LiteralBuilder(Object literal) {
    this.literal = literal;
    this.intervalValue = null;
    this.intervalSign = null;
    this.intervalField = null;
  }

  LiteralBuilder(
      String value,
      IntervalSign sign,
      IntervalField field
  ) {
    this.literal = null;
    this.intervalValue = value;
    this.intervalSign = sign;
    this.intervalField = field;
  }

  @Override
  Expression build() {
    if (null != intervalSign) {
      return new IntervalLiteral(intervalValue, intervalSign.getSign(), intervalField.getField());
    } else if (null == literal) {
      return new NullLiteral();
    } else if (literal instanceof Boolean) {
      return new BooleanLiteral(literal.toString());
    } else if (literal instanceof Character) {
      return new CharLiteral(literal.toString());
    } else if ((literal instanceof Float) || (literal instanceof Double)) {
      return new DoubleLiteral(literal.toString());
    } else if (literal instanceof Byte) {
      return new GenericLiteral("TINYINT", literal.toString());
    } else if ((literal instanceof Integer) || (literal instanceof Long)) {
      return new LongLiteral(literal.toString());
    } else if (literal instanceof Timestamp) {
      return new TimestampLiteral(literal.toString());
    } else if (literal instanceof String) {
      return new StringLiteral(literal.toString());
    } else {
      throw new UnsupportedOperationException("Unrecognized literal type.");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LiteralBuilder that = (LiteralBuilder) o;
    return Objects.equals(literal, that.literal) &&
        Objects.equals(intervalValue, that.intervalValue) &&
        intervalSign == that.intervalSign &&
        intervalField == that.intervalField;
  }

  @Override
  public int hashCode() {
    return Objects.hash(literal, intervalValue, intervalSign, intervalField);
  }
}
