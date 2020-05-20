package com.rapid7.presto;

import com.facebook.presto.sql.tree.IntervalLiteral;

public enum IntervalField {
  YEAR,
  MONTH,
  DAY,
  HOUR,
  MINUTE,
  SECOND;

  IntervalLiteral.IntervalField getField() {
    switch (this) {
      case YEAR:
        return IntervalLiteral.IntervalField.YEAR;
      case MONTH:
        return IntervalLiteral.IntervalField.MONTH;
      case DAY:
        return IntervalLiteral.IntervalField.DAY;
      case HOUR:
        return IntervalLiteral.IntervalField.HOUR;
      case MINUTE:
        return IntervalLiteral.IntervalField.MINUTE;
      case SECOND:
        return IntervalLiteral.IntervalField.SECOND;
      default:
        throw new UnsupportedOperationException("That sign does not exist.");
    }
  }
}
