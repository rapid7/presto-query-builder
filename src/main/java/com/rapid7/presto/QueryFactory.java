// ===========================================================================
// COPYRIGHT (C) 2019, Rapid7 LLC, Boston, MA, USA.
// ---------------------------------------------------------------------------
// This code is licensed under MIT license (see LICENSE for details)
// ===========================================================================
package com.rapid7.presto;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Expression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryFactory {
  private final Map<String, Integer> parameterMap = new HashMap<>();
  private final List<Expression> parameters = new ArrayList<>();

  public Map<String, Integer> getParameterMap() {
    return parameterMap;
  }

  public void setParam(String key, ExpressionBuilder value) {
    if (!parameterMap.containsKey(key)) {
      parameterMap.put(key, parameters.size());
      parameters.add(value.build());
    } else {
      parameters.set(parameterMap.get(key), value.build());
    }
  }

  public String build(QueryBuilder builder) {
    return SqlFormatter.formatSql(builder.build(), Optional.of(parameters)).trim();
  }
}
