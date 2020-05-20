package com.rapid7.presto;

import com.facebook.presto.sql.tree.SortItem;

public enum SortNullOrdering {
  FIRST,
  LAST,
  UNDEFINED;

  SortItem.NullOrdering getNullOrdering() {
    switch (this) {
      case FIRST:
        return SortItem.NullOrdering.FIRST;
      case LAST:
        return SortItem.NullOrdering.LAST;
      case UNDEFINED:
        return SortItem.NullOrdering.UNDEFINED;
      default:
        throw new UnsupportedOperationException("That sign does not exist.");
    }
  }
}
