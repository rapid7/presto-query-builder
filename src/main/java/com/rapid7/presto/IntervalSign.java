package com.rapid7.presto;

import com.facebook.presto.sql.tree.IntervalLiteral;

public enum IntervalSign {
  POSITIVE,
  NEGATIVE;

  IntervalLiteral.Sign getSign() {
    switch (this) {
      case POSITIVE:
        return IntervalLiteral.Sign.POSITIVE;
      case NEGATIVE:
        return IntervalLiteral.Sign.NEGATIVE;
      default:
        throw new UnsupportedOperationException("That sign does not exist.");
    }
  }
}
