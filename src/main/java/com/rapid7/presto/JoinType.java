package com.rapid7.presto;

import com.facebook.presto.sql.tree.Join;

public enum JoinType {
  CROSS,
  INNER,
  LEFT,
  RIGHT,
  FULL,
  IMPLICIT;

  Join.Type getJoinType() {
    switch (this) {
      case CROSS:
        return Join.Type.CROSS;
      case INNER:
        return Join.Type.INNER;
      case LEFT:
        return Join.Type.LEFT;
      case RIGHT:
        return Join.Type.RIGHT;
      case FULL:
        return Join.Type.FULL;
      case IMPLICIT:
        return Join.Type.IMPLICIT;
      default:
        throw new UnsupportedOperationException("That sign does not exist.");
    }
  }
}
