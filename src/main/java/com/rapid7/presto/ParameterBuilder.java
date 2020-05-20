package com.rapid7.presto;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Parameter;
import java.util.Map;
import java.util.Objects;

public class ParameterBuilder extends ExpressionBuilder {
  private Map<String, Integer> parameterMap;
  private String key;

  ParameterBuilder(Map<String, Integer> parameterMap, String key) {
    this.parameterMap = parameterMap;
    this.key = key;
  }

  @Override
  Expression build() {
    if (!parameterMap.containsKey(key)) {
      throw new UnsupportedOperationException("A parameter with this name does not exist");
    }

    return new Parameter(parameterMap.get(key));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ParameterBuilder that = (ParameterBuilder) o;
    return parameterMap.equals(that.parameterMap) &&
        key.equals(that.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parameterMap, key);
  }
}
