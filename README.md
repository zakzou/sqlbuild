sqlbuild
========

Generate the SQL statement  golang


## Documentation

```go
mysql := sqlbuild.NewMysqlAdapter()
buildQuery := sqlbuild.NewBuildQuery(mysql, "")

buildQuery.Select("user_table").Where("id > ?", 0).Limit(5).Offset(2)

fmt.Println(buildQuery)
```
