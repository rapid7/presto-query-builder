package com.rapid7.presto;


import com.facebook.presto.sql.tree.Expression;

public abstract class ExpressionBuilder {
  abstract Expression build();
}
