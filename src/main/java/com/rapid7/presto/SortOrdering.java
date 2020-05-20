package com.rapid7.presto;

import com.facebook.presto.sql.tree.SortItem;

public enum SortOrdering {
  ASCENDING,
  DESCENDING;

  SortItem.Ordering getOrdering() {
    switch (this) {
      case ASCENDING:
        return SortItem.Ordering.ASCENDING;
      case DESCENDING:
        return SortItem.Ordering.DESCENDING;
      default:
        throw new UnsupportedOperationException("That sign does not exist.");
    }
  }
}
