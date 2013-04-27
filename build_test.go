package sqlbuild

import (
	"testing"
)

func Test_Select(t *testing.T) {
	mysql := NewMysqlAdapter()
	sqlquery := NewBuildQuery(mysql, "")
	sqlquery.Reset().Select("user_0").Where("user_0.id > ?", 10000).Where("user_0.id < ?", 10004).Join("user_info_0", "user_info_0.id = user_0.id", InnerJoin).Order("user_0.id", SortAsc)
	result := "SELECT * FROM user_0 INNER JOIN user_info_0 ON user_info_0.`id` = user_0.`id` WHERE (user_0.`id` > '10000') AND (user_0.`id` < '10004') ORDER BY user_0.`id` ASC"
	if sqlquery.String() != result {
		t.Error("select error")
	}
}

func Test_Insert(t *testing.T) {
	mysql := NewMysqlAdapter()
	sqlquery := NewBuildQuery(mysql, "")
	sqlquery.Reset().Insert("user_0").Value("username", "zwc")
	result := "INSERT INTO user_0 (`username`) VALUES ('zwc')"
	if sqlquery.String() != result {
		t.Error("insert error")
	}
}
