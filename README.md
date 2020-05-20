# PRESTO-QUERY-BUILDER

This library allows you to programmatically generate queries for Presto to enable dynamically generated queries.

## Features:

#### SELECT
* Union
* With
* Where
* Order By
* Group By
* Join

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
    new SelectQueryBuilder(
    ).with(
        "with_table"
    ).select(
        false,
        projection("aliasa", ref("alias.a"))
    ).from(
        aliasedTable("alias", "b")
    ).end(
    ).select(
        false,
        projection("t1a", ref("t1.a")),
        projection("t2aliasa", ref("t2.aliasa"))
    )
    .from(
        aliasedTable("t1", "a")
    )
    .join(
        INNER,
        aliasedTable("t2", "with_table"),
        equal(ref("t1.a"), ref("t2.aliasa"))
    )
);
```

Insert:
```
QueryFactory qf = new QueryFactory();
qf.setParam("a", lit(1));
qf.setParam("b", lit(2));
qf.setParam("c", lit(3));

String query = qf.build(
    new InsertQueryBuilder(
    ).into(
        qualifiedName("t1"),
        Arrays.asList(
            identifier("a"),
            identifier("b"),
            identifier("c")
        )
    ).row(
    ).value(
        "a",
        QueryUtils.param(qf, "a")
    ).value(
        "b",
        QueryUtils.param(qf, "b")
    ).value(
        "c",
        QueryUtils.param(qf, "c")
    ).end()
);
```

Delete:
```
QueryFactory qf = new QueryFactory();
qf.setParam("a", lit(1));

String query = qf.build(
    new DeleteQueryBuilder(
    ).from(
        table("t")
    ).where(
        equal(ref("a"), QueryUtils.param(qf, "a"))
    )
);
```

## Special Notes
* Currently, this is pinned to Presto 0.227 but could probably be used with newer versions as well.
* Most Presto connectors do not support INSERT and/or DELETE operations so the are not very feature rich.
* Since you can always DELETE then INSERT, UPDATE was not prioritized. However, it's still on the TODO list.

## TODO
* add more tests
* query builder for update queries
