package sqlbuild

import (
	"fmt"
	"strings"
	"unicode"
)

const (
	KeyWords     = "|*|PRIMARY|AND|OR|LIKE|BINARY|BY|DISTINCT|AS|IN|IS|NULL|"
	InnerJoin    = "INNER"
	OuterJoin    = "OUTER"
	LeftJoin     = "LEFT"
	RightJoin    = "RIGHT"
	SelectAction = "SELECT"
	UpdateAction = "UPDATE"
	InsertAction = "Insert"
	DeleteAction = "Delete"
	SortDesc     = "DESC"
	SortAsc      = "ASC"
)

type SqlPreBuild struct {
	action, table, fields, where, limit, offset string
	join, order                                 [][]string
	group                                       []string
	rows                                        map[string]string
}

func NewSqlPreBuild() *SqlPreBuild {
	return &SqlPreBuild{
		fields: "*",
		rows:   map[string]string{},
		join:   [][]string{},
		order:  [][]string{},
		group:  []string{},
	}
}

type BuildQuery struct {
	adapter     Adapter
	prefix      string
	sqlPreBuild *SqlPreBuild
}

func NewBuildQuery(adapter Adapter, prefix string) *BuildQuery {
	this := new(BuildQuery)
	this.prefix = prefix
	this.sqlPreBuild = NewSqlPreBuild()
	this.adapter = adapter
	return this
}

func (this *BuildQuery) isAlnum(v interface{}) bool {
	switch t := v.(type) {
	case rune:
		return unicode.IsDigit(t) || unicode.IsLetter(t)
	case string:
		r := []rune(t)
		for i := 0; i < len(r); i++ {
			if !unicode.IsDigit(r[i]) && !unicode.IsLetter(r[i]) {
				return false
			}
		}
	default:
		return false
	}
	return true
}

func (this *BuildQuery) isDigit(v interface{}) bool {
	switch t := v.(type) {
	case rune:
		return unicode.IsDigit(t)
	case string:
		r := []rune(t)
		for i := 0; i < len(r); i++ {
			if !unicode.IsDigit(r[i]) {
				return false
			}
		}
	default:
		return false
	}
	return true
}

func (this *BuildQuery) filterPrefix(str string) string {
	if strings.HasPrefix(str, "table.") {
		return strings.Replace(str, "table.", this.prefix, 1)
	}
	return str
}

func (this *BuildQuery) filterColum(str string) string {
	r := []rune(str + " 0")
	lastIsAlnum := false
	result, word, split := "", "", ""
	quotes := 0

	for i := 0; i < len(r); i++ {
		char := r[i]
		if this.isAlnum(char) || strings.ContainsRune("_*", char) {
			if !lastIsAlnum {
				if quotes > 0 && !this.isDigit(word) && "." != split && !strings.Contains(KeyWords, strings.ToUpper(fmt.Sprintf("|%s|", word))) {
					word = this.adapter.QuoteColumn(word)
				} else if "." == split && "table" == word {
					word = this.prefix
					split = ""
				}
				result += word + split
				word = ""
				quotes = 0
			}
			word += string(char)
			lastIsAlnum = true
		} else {
			if lastIsAlnum {
				if quotes == 0 {
					if strings.ContainsRune(" ,)=<>.+-*/", char) {
						quotes = 1
					} else if '(' == char {
						quotes = -1
					}
				}
				split = ""
			}
			split += string(char)
			lastIsAlnum = false
		}
	}
	return strings.TrimSpace(result)
}

func (this *BuildQuery) getColumnFromParameters(v ...string) string {
	fields := []string{}
	for _, value := range v {
		value = strings.Replace(value, "as", "AS", 1)
		value = strings.Replace(value, "As", "AS", 1)
		fields = append(fields, value)
	}
	return this.filterColum(strings.Join(fields, " , "))
}

func (this *BuildQuery) Join(table, condition, op string) *BuildQuery {
	table = this.filterPrefix(table)
	condition = this.filterColum(condition)
	this.sqlPreBuild.join = append(this.sqlPreBuild.join, []string{table, condition, op})
	return this
}

func (this *BuildQuery) Reset() *BuildQuery {
	this.sqlPreBuild = NewSqlPreBuild()
	return this
}

func (this *BuildQuery) Where(v ...interface{}) *BuildQuery {
	condition := v[0].(string)
	condition = strings.Replace(this.filterColum(condition), "?", "%v", -1)
	var operator string
	if this.sqlPreBuild.where == "" {
		operator = " WHERE "
	} else {
		operator = " AND "
	}
	if len(v) > 1 {
		params := []interface{}{}
		for _, value := range v[1:] {
			value = fmt.Sprintf("%v", value)
			params = append(params, this.adapter.QuoteValue(value.(string)))
		}
		condition = fmt.Sprintf(condition, params...)
	}
	this.sqlPreBuild.where = fmt.Sprintf("%s%s(%s)", this.sqlPreBuild.where, operator, condition)
	return this
}

func (this *BuildQuery) OrWhere(v ...interface{}) *BuildQuery {
	condition := v[0].(string)
	condition = strings.Replace(this.filterColum(condition), "?", "%v", -1)
	var operator string
	if this.sqlPreBuild.where == "" {
		operator = " WHERE "
	} else {
		operator = " OR "
	}
	if len(v) > 1 {
		params := []interface{}{}
		for _, value := range v[1:] {
			value = fmt.Sprintf("%v", value)
			params = append(params, this.adapter.QuoteValue(value.(string)))
		}
		condition = fmt.Sprintf(condition, params...)
	}
	this.sqlPreBuild.where = fmt.Sprintf("%s%s (%s)", this.sqlPreBuild.where, operator, condition)
	return this
}

func (this *BuildQuery) Limit(limit int) *BuildQuery {
	this.sqlPreBuild.limit = fmt.Sprintf("%d", limit)
	return this
}

func (this *BuildQuery) Offset(offset int) *BuildQuery {
	this.sqlPreBuild.offset = fmt.Sprintf("%d", offset)
	return this
}

func (this *BuildQuery) Page(pageno, pagesize int) *BuildQuery {
	this.Limit(pagesize)
	this.Offset((pageno - 1) * pagesize)
	return this
}

func (this *BuildQuery) Order(orderBy, sort string) *BuildQuery {
	orderBy = this.filterColum(orderBy)
	this.sqlPreBuild.order = append(this.sqlPreBuild.order, []string{orderBy, sort})
	return this
}

func (this *BuildQuery) Group(key string) *BuildQuery {
	key = this.filterColum(key)
	this.sqlPreBuild.group = append(this.sqlPreBuild.group, key)
	return this
}

func (this *BuildQuery) Fields(v ...string) *BuildQuery {
	this.sqlPreBuild.fields = this.getColumnFromParameters(v...)
	return this
}

func (this *BuildQuery) Select(table string) *BuildQuery {
	this.sqlPreBuild.action = SelectAction
	this.sqlPreBuild.table = this.filterPrefix(table)
	return this
}

func (this *BuildQuery) Update(table string) *BuildQuery {
	this.sqlPreBuild.action = UpdateAction
	this.sqlPreBuild.table = this.filterPrefix(table)
	return this
}

func (this *BuildQuery) Insert(table string) *BuildQuery {
	this.sqlPreBuild.action = InsertAction
	this.sqlPreBuild.table = this.filterPrefix(table)
	return this
}

func (this *BuildQuery) Delete(table string) *BuildQuery {
	this.sqlPreBuild.action = DeleteAction
	this.sqlPreBuild.table = this.filterPrefix(table)
	return this
}

func (this *BuildQuery) Value(key string, value interface{}) *BuildQuery {
	key = this.filterColum(key)
	newValue := this.adapter.QuoteValue(fmt.Sprintf("%v", value))
	this.sqlPreBuild.rows[key] = newValue
	return this
}

func (this *BuildQuery) Values(dict map[string]interface{}) *BuildQuery {
	for key, value := range dict {
		key = this.filterColum(key)
		newValue := this.adapter.QuoteValue(fmt.Sprintf("%v", value))
		this.sqlPreBuild.rows[key] = newValue
	}
	return this
}

func (this *BuildQuery) String() string {
	switch this.sqlPreBuild.action {
	case SelectAction:
		return this.adapter.ParseSelect(this.sqlPreBuild)
	case InsertAction:
		keys, values := []string{}, []string{}
		for key, value := range this.sqlPreBuild.rows {
			keys = append(keys, key)
			values = append(values, value)
		}
		keyString := fmt.Sprintf(" (%s)", strings.Join(keys, " , "))
		valueString := fmt.Sprintf(" (%s)", strings.Join(values, " , "))
		return fmt.Sprintf("INSERT INTO %s%s VALUES%s", this.sqlPreBuild.table, keyString, valueString)
	case DeleteAction:
		return fmt.Sprintf("DELETE FROM %s%s", this.sqlPreBuild.table, this.sqlPreBuild.where)
	case UpdateAction:
		columns := []string{}
		for key, value := range this.sqlPreBuild.rows {
			columns = append(columns, fmt.Sprintf("%s = %s", key, value))
		}
		params := strings.Join(columns, " , ")
		return fmt.Sprintf("UPDATE %s SET %s %s", this.sqlPreBuild.table, params, this.sqlPreBuild.where)
	}
	return ""
}
