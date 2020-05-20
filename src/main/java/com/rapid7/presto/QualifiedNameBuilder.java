package com.rapid7.presto;

import com.facebook.presto.sql.tree.QualifiedName;
import java.util.Objects;

public class QualifiedNameBuilder {
  private String catalog;
  private String schema;
  private String fieldName;

  QualifiedNameBuilder(String catalog, String schema, String fieldName) {
    this.catalog = "\"" + catalog + "\"";
    this.schema = "\"" + schema + "\"";
    this.fieldName = "\"" + fieldName + "\"";
  }

  QualifiedName build() {
    if (null == catalog && null == schema) {
      return QualifiedName.of(fieldName);
    } else if (null == catalog) {
      return QualifiedName.of(schema, fieldName);
    } else {
      return QualifiedName.of(catalog, schema, fieldName);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QualifiedNameBuilder that = (QualifiedNameBuilder) o;
    return Objects.equals(catalog, that.catalog) &&
        Objects.equals(schema, that.schema) &&
        fieldName.equals(that.fieldName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalog, schema, fieldName);
  }
}
