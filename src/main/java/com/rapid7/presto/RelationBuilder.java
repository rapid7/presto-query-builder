package com.rapid7.presto;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Table;
import java.util.Arrays;
import java.util.Objects;

public class RelationBuilder {
  private String alias;
  private String name;

  RelationBuilder(String name) {
    this.alias = null;
    this.name = name;
  }

  RelationBuilder(String alias, String name) {
    this.alias = alias;
    this.name = name;
  }

  Relation build() {
    String[] parts = name.split("\\.");
    String[] rest = new String[] {};
    if (parts.length > 1) {
      rest = Arrays.copyOfRange(parts, 1, parts.length);
    }

    Relation table = new Table(QualifiedName.of(parts[0], rest));
    if (null != alias) {
      table = new AliasedRelation(table, new Identifier(alias), null);
    }

    return table;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RelationBuilder that = (RelationBuilder) o;
    return Objects.equals(alias, that.alias) &&
        name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, name);
  }
}
