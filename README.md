# PRESTO-QUERY-BUILDER

This library allows you to programmatically generate queries for Presto to enable dynamically generated queries.

## Features:

#### SELECT
* Union
* With
* Where
* Order By
* Group By

#### INSERT
* Mult-row inserts

#### DELETE
* Where

## Configuration

None required.

## Usage
Select:

```
QueryFactory qf = new QueryFactory();
String query = qf.build(
    new SelectQueryBuilder()
        .select(
            false,
            projection("aliasa", ref("alias.a"))
        ).from(aliasedTable("alias", "b"))
        .with("with_table")
        .select(
            false,
            projection("t1a", ref("t1.a")),
            projection("t2aliasa", ref("t2.aliasa"))
        )
        .from(aliasedTable("t1", "a"))
        .join(INNER, aliasedTable("t2", "with_table"), equal(ref("t1.a"), ref("t2.aliasa")))
);
```

Insert:
```
QueryFactory qf = new QueryFactory();
qf.setParam("a", lit(1));
qf.setParam("b", lit(2));
qf.setParam("c", lit(3));

String query = qf.build(
    new InsertQueryBuilder()
        .into(
            qualifiedName("t1"),
            Arrays.asList(
                identifier("a"),
                identifier("b"),
                identifier("c")
            )
        )
        .value("a", qf.param("a"))
        .value("b", qf.param("b"))
        .value("c", qf.param("c"))
        .row()
);
```

Delete:
```
QueryFactory qf = new QueryFactory();
qf.setParam("a", lit(1));

String query = qf.build(
    new DeleteQueryBuilder()
        .from(
            table("t")
        )
        .where(
            equal(ref("a"), qf.param("a"))
            )
    );
```

## Special Notes
* Currently, this is pinned to Presto 0.206 but could probably be used with newer versions as well.
* Most Presto connectors do not support INSERT and/or DELETE operations so the are not very feature rich.
* Since you can always DELETE then INSERT, UPDATE was not prioritized. However, it's still on the TODO list.

## TODO
* add more tests
* query builder for update queries
