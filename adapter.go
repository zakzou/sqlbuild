package sqlbuild

import (
	"fmt"
	"strings"
)

type Adapter interface {
	QuoteColumn(str string) string
	QuoteValue(str string) string
	ParseSelect(sql *SqlPreBuild) string
}

type MysqlAdapter struct {
}

func NewMysqlAdapter() *MysqlAdapter {
	return new(MysqlAdapter)
}

func (this *MysqlAdapter) QuoteValue(str string) string {
	str = strings.Replace(str, "'", "''", -1)
	str = strings.Replace(str, "\\", "\\\\", -1)
	return fmt.Sprintf("'%s'", str)
}

func (this *MysqlAdapter) QuoteColumn(str string) string {
	return fmt.Sprintf("`%s`", str)
}

func (this *MysqlAdapter) ParseSelect(sql *SqlPreBuild) string {
	for _, values := range sql.join {
		sql.table = fmt.Sprintf("%s %s JOIN %s ON %s", sql.table, values[2], values[0], values[1])
	}
	if sql.limit != "" {
		sql.limit = " LIMIT " + sql.limit
	}
	if sql.offset != "" {
		sql.offset = " OFFSET " + sql.offset
	}
	orderby := []string{}
	for _, values := range sql.order {
		orderby = append(orderby, fmt.Sprintf("%s %s", values[0], values[1]))
	}
	orderbyString := ""
	if len(orderby) > 0 {
		orderbyString = fmt.Sprintf(" ORDER BY %s", strings.Join(orderby, ", "))
	}
	groupbyString := ""
	if len(sql.group) > 0 {
		groupbyString = fmt.Sprintf(" GROUP BY %s", strings.Join(sql.group, ", "))
	}
	return fmt.Sprintf("SELECT %s FROM %s%s%s%s%s%s", sql.fields, sql.table, sql.where, groupbyString, orderbyString, sql.limit, sql.offset)
}

type SqliteAdapter struct {
}

func NewSqliteAdapter() *SqliteAdapter {
	return new(SqliteAdapter)
}

func (this *SqliteAdapter) QuoteValue(str string) string {
	str = strings.Replace(str, "'", "''", -1)
	return fmt.Sprintf("'%s'", str)
}

func (this *SqliteAdapter) QuoteColumn(str string) string {
	return fmt.Sprintf("\"%s\"", str)
}

func (this *SqliteAdapter) ParseSelect(sql *SqlPreBuild) string {
	for _, values := range sql.join {
		sql.table = fmt.Sprintf("%s %s JOIN %s ON %s", sql.table, values[2], values[0], values[1])
	}
	if sql.limit != "" {
		sql.limit = " LIMIT " + sql.limit
	}
	if sql.offset != "" {
		sql.offset = " OFFSET " + sql.offset
	}
	orderby := []string{}
	for _, values := range sql.order {
		orderby = append(orderby, fmt.Sprintf("%s %s", values[0], values[1]))
	}
	orderbyString := ""
	if len(orderby) > 0 {
		orderbyString = fmt.Sprintf(" ORDER BY %s", strings.Join(orderby, ", "))
	}
	groupbyString := ""
	if len(sql.group) > 0 {
		groupbyString = fmt.Sprintf(" GROUP BY %s", strings.Join(sql.group, ", "))
	}
	return fmt.Sprintf("SELECT %s FROM %s%s%s%s%s%s", sql.fields, sql.table, sql.where, groupbyString, orderbyString, sql.limit, sql.offset)
}

type PgsqlAdapter struct {
}

func NewPgsqlAdapter() *PgsqlAdapter {
	return new(PgsqlAdapter)
}

func (this *PgsqlAdapter) QuoteValue(str string) string {
	str = strings.Replace(str, "'", "\\'", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "\\", "\\\\", -1)
	return str
}

func (this *PgsqlAdapter) QuoteColumn(str string) string {
	return fmt.Sprintf("\"%s\"", str)
}

func (this *PgsqlAdapter) ParseSelect(sql *SqlPreBuild) string {
	for _, values := range sql.join {
		sql.table = fmt.Sprintf("%s %s JOIN %s ON %s", sql.table, values[2], values[0], values[1])
	}
	if sql.limit != "" {
		sql.limit = " LIMIT " + sql.limit
	}
	if sql.offset != "" {
		sql.offset = " OFFSET " + sql.offset
	}
	orderby := []string{}
	for _, values := range sql.order {
		orderby = append(orderby, fmt.Sprintf("%s %s", values[0], values[1]))
	}
	orderbyString := ""
	if len(orderby) > 0 {
		orderbyString = fmt.Sprintf(" ORDER BY %s", strings.Join(orderby, ", "))
	}
	groupbyString := ""
	if len(sql.group) > 0 {
		groupbyString = fmt.Sprintf(" GROUP BY %s", strings.Join(sql.group, ", "))
	}
	return fmt.Sprintf("SELECT %s FROM %s%s%s%s%s%s", sql.fields, sql.table, sql.where, groupbyString, orderbyString, sql.limit, sql.offset)
}
