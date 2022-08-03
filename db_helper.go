package db_helper

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/gorp.v1"
)

//var dbmap *gorp.DbMap

type DbConnection struct {
	Name  string
	DbMap *gorp.DbMap
}

func (d *DbConnection) Execute(sql string) (err error) {
	_, err = d.DbMap.Exec(sql)
	return
}

func (d *DbConnection) Query(i interface{}, queryStr string) (err error) {
	_, err = d.DbMap.Select(i, queryStr)
	return
}

func (d *DbConnection) QueryInt(queryStr string) (rs int64, err error) {
	rs, err = d.DbMap.SelectInt(queryStr)
	return
}

func (d *DbConnection) CreateQueryExecutor(qb *QueryBuilder) *QueryExecutor {
	return &QueryExecutor{
		qb:     qb,
		dbConn: d,
	}
}

var dbConnMap map[string]*DbConnection

func init() {
	dbConnMap = map[string]*DbConnection{}
}

func GetDefDb() *DbConnection {
	return GetDb("default")
}

func GetDb(name string) *DbConnection {
	return dbConnMap[name]
}

func InitDbConnection(name string, dbSource string, maxConn, maxIdle, lifeTime int) (err error) {
	if _, exist := dbConnMap[name]; exist {
		return
	}

	db, err := sql.Open("mysql", dbSource)
	if err != nil {
		return
	}

	err = db.Ping()
	if err != nil {
		return
	}

	db.SetMaxOpenConns(maxConn)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(time.Duration(lifeTime) * time.Second)

	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{}}

	dbConnMap[name] = &DbConnection{
		Name:  name,
		DbMap: dbmap,
	}

	return

}

//
// old version

//lifeTime should less than mysql's wait_time
func InitDb(dbSource string, maxConn, maxIdle, lifeTime int) (err error) {
	return InitDbConnection("default", dbSource, maxConn, maxIdle, lifeTime)

}

func Execute(sql string) (err error) {
	return GetDefDb().Execute(sql)
}

func Query(i interface{}, queryStr string) (err error) {
	return GetDefDb().Query(i, queryStr)
}

func QueryInt(queryStr string) (rs int64, err error) {
	return GetDefDb().QueryInt(queryStr)
}

type QueryExecutor struct {
	qb     *QueryBuilder
	dbConn *DbConnection
}

func CreateQueryExecutor(qb *QueryBuilder) *QueryExecutor {
	return &QueryExecutor{
		qb:     qb,
		dbConn: GetDefDb(),
	}
}

func (q *QueryExecutor) FindAll(i interface{}) (err error) {
	_, err = q.dbConn.DbMap.Select(i, q.qb.ToQueryStr())
	return
}

func (q *QueryExecutor) FindOne(i interface{}) (err error) {
	err = q.dbConn.DbMap.SelectOne(i, q.qb.ToQueryStr())
	return
}

func (q *QueryExecutor) Insert() (int64, error) {
	res, err := q.dbConn.DbMap.Exec(q.qb.ToInsertStr())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (q *QueryExecutor) Count() (rs int64, err error) {
	rs, err = q.dbConn.DbMap.SelectInt(q.qb.ToQueryStr())
	return
}

func (q *QueryExecutor) Update() (err error) {
	_, err = q.dbConn.DbMap.Exec(q.qb.ToUpdateStr())
	return
}

func (q *QueryExecutor) Delete() (err error) {
	_, err = q.dbConn.DbMap.Exec(q.qb.ToDeleteStr())
	return
}

type QueryOrder struct {
	By        string
	Direction string
}

type QueryLimit struct {
	Offset int
	Count  int
}

type QueryWhere struct {
	Col   string
	Op    string
	Param interface{}
}

type QueryAdd struct {
	Col   string
	Param interface{}
}

type QuerySet struct {
	Col   string
	Param interface{}
}

type QueryBuilder struct {
	Table       string
	QueryFields string
	QueryAdds   []QueryAdd
	QuerySets   []QuerySet
	QueryOrders []QueryOrder
	QueryLimit  QueryLimit
	QueryWheres []QueryWhere
	QueryGroup  string
}

func CreateQueryBuilder(table string) *QueryBuilder {
	return &QueryBuilder{
		Table: table,
	}
}

func UnserializeQueryBuilder(str string) (qb *QueryBuilder, err error) {
	qb = &QueryBuilder{}
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, qb)
	return
}

func (q *QueryBuilder) Serialize() (rs string, err error) {
	b, err := json.Marshal(q)
	if err != nil {
		return
	}
	rs = base64.StdEncoding.EncodeToString(b)
	return
}

func (q *QueryBuilder) Add(col string, param interface{}) *QueryBuilder {
	q.QueryAdds = append(q.QueryAdds, QueryAdd{
		Col:   col,
		Param: param,
	})
	return q
}

func (q *QueryBuilder) Set(col string, param interface{}) *QueryBuilder {
	q.QuerySets = append(q.QuerySets, QuerySet{
		Col:   col,
		Param: param,
	})
	return q
}

func (q *QueryBuilder) Fields(fields string) *QueryBuilder {
	q.QueryFields = fields
	return q
}

func (q *QueryBuilder) Group(cols string) *QueryBuilder {
	q.QueryGroup = cols
	return q
}

func (q *QueryBuilder) Order(by string, direction string) *QueryBuilder {
	q.QueryOrders = append(q.QueryOrders, QueryOrder{
		By:        by,
		Direction: direction,
	})
	return q
}

func (q *QueryBuilder) Limit(offset int, count int) *QueryBuilder {
	q.QueryLimit.Offset = offset
	q.QueryLimit.Count = count
	return q
}

func (q *QueryBuilder) NextPage() *QueryBuilder {
	if q.QueryLimit.Count > 0 {
		q.QueryLimit.Offset += q.QueryLimit.Count
	}
	return q
}

func (q *QueryBuilder) Where(col string, op string, param interface{}) *QueryBuilder {
	qh := QueryWhere{
		Col:   col,
		Op:    op,
		Param: param,
	}
	q.QueryWheres = append(q.QueryWheres, qh)
	return q
}

func (q *QueryBuilder) ToInsertStr() (rs string) {
	addColArr := []string{}
	addValArr := []string{}

	for _, qa := range q.QueryAdds {
		addColArr = append(addColArr, fmt.Sprintf("`%s`", qa.Col))
		switch qa.Param.(type) {
		case string:
			addValArr = append(addValArr, fmt.Sprintf("'%s'", strings.Replace(qa.Param.(string), "'", "\\'", -1)))
		default:
			addValArr = append(addValArr, fmt.Sprintf("%v", qa.Param))
		}
	}

	rs = fmt.Sprintf("INSERT INTO `%s` (%s) VALUES(%s)", q.Table, strings.Join(addColArr, ","), strings.Join(addValArr, ","))
	return
}

func (q *QueryBuilder) ToUpdateStr() (rs string) {
	setArr := []string{}
	for _, qs := range q.QuerySets {
		str := ""
		switch qs.Param.(type) {
		case string:
			str = fmt.Sprintf("`%s`='%s'", qs.Col, strings.Replace(qs.Param.(string), "'", "\\'", -1))
		default:
			str = fmt.Sprintf("`%s`=%v", qs.Col, qs.Param)
		}

		setArr = append(setArr, str)
	}
	setStr := ""
	if len(setArr) > 0 {
		setStr = "SET " + strings.Join(setArr, ",")
	}

	whereStr := q.makeWhereStr()

	rs = fmt.Sprintf("UPDATE `%s` %s %s", q.Table, setStr, whereStr)
	return
}

func (q *QueryBuilder) ToDeleteStr() (rs string) {
	whereStr := q.makeWhereStr()

	rs = fmt.Sprintf("DELETE FROM `%s` %s", q.Table, whereStr)
	return
}

func (q *QueryBuilder) makeOrderStr() string {
	orderArr := []string{}
	for _, qo := range q.QueryOrders {
		str := fmt.Sprintf("`%s` %s", qo.By, strings.ToUpper(qo.Direction))
		orderArr = append(orderArr, str)
	}

	orderStr := ""
	if len(orderArr) > 0 {
		orderStr = " ORDER BY " + strings.Join(orderArr, ",")
	}
	return orderStr
}

func (q *QueryBuilder) makeLimitStr() string {
	limitStr := ""
	if q.QueryLimit.Count > 0 {
		limitStr = fmt.Sprintf(" LIMIT %d, %d", q.QueryLimit.Offset, q.QueryLimit.Count)
	}
	return limitStr
}

func (q *QueryBuilder) makeGroupStr() string {
	rs := ""
	if q.QueryGroup != "" {
		rs = fmt.Sprintf(" GROUP BY `%s`", strings.Replace(q.QueryGroup, ",", "`,`", -1))
	}
	return rs
}

func (q *QueryBuilder) makeWhereStr() string {
	re := regexp.MustCompile(`('|\\)`)
	whereArr := []string{}
	for _, qw := range q.QueryWheres {
		str := ""
		switch qw.Param.(type) {
		case []string:
			tmpItems := qw.Param.([]string)
			items := make([]string, len(tmpItems), len(tmpItems))
			for i, _ := range items {
				items[i] = "'" + re.ReplaceAllString(tmpItems[i], `\$1`) + "'"
			}
			str = fmt.Sprintf("`%s` %s (%s)", qw.Col, qw.Op, strings.Join(items, ","))
		case []int:
			tmpItems := qw.Param.([]int)
			items := make([]string, len(tmpItems), len(tmpItems))
			for i, _ := range tmpItems {
				items[i] = strconv.Itoa(tmpItems[i])
			}
			str = fmt.Sprintf("`%s` %s (%s)", qw.Col, qw.Op, strings.Join(items, ","))
		case []int64:
			tmpItems := qw.Param.([]int64)
			items := make([]string, len(tmpItems), len(tmpItems))
			for i, _ := range tmpItems {
				items[i] = strconv.FormatInt(tmpItems[i], 64)
			}
			str = fmt.Sprintf("`%s` %s (%s)", qw.Col, qw.Op, strings.Join(items, ","))
		case []float64:
			tmpItems := qw.Param.([]float64)
			items := make([]string, len(tmpItems), len(tmpItems))
			for i, _ := range tmpItems {
				items[i] = strconv.FormatFloat(tmpItems[i], 'E', -1, 64)
			}
			str = fmt.Sprintf("`%s` %s (%s)", qw.Col, qw.Op, strings.Join(items, ","))
		case string:
			item := qw.Param.(string)
			item = "'" + re.ReplaceAllString(item, `\$1`) + "'"
			str = fmt.Sprintf("`%s` %s %s", qw.Col, qw.Op, item)
		case int:
			str = fmt.Sprintf("`%s` %s %d", qw.Col, qw.Op, qw.Param.(int))
		case int64:
			str = fmt.Sprintf("`%s` %s %d", qw.Col, qw.Op, qw.Param.(int64))
		case float64:
			str = fmt.Sprintf("`%s` %s %f", qw.Col, qw.Op, qw.Param.(float64))
		default:
		}

		whereArr = append(whereArr, str)
	}

	//sort.Strings(whereArr)

	whereStr := ""
	if len(whereArr) > 0 {
		whereStr = " WHERE " + strings.Join(whereArr, " AND ")
	}
	return whereStr
}

func (q *QueryBuilder) makeFieldStr() string {
	fieldStr := "*"
	if q.QueryFields != "" && q.QueryFields != fieldStr {
		fieldArr := []string{}
		items := strings.Split(q.QueryFields, ",")
		for _, item := range items {
			if strings.Index(item, "(") > 0 {
				fieldArr = append(fieldArr, item)
			} else {
				fieldArr = append(fieldArr, fmt.Sprintf("`%s`", item))
			}
		}
		fieldStr = strings.Join(fieldArr, ",")
	}
	return fieldStr
}

func (q *QueryBuilder) ToQueryStr() (rs string) {
	whereStr := q.makeWhereStr()

	orderStr := q.makeOrderStr()

	fieldStr := q.makeFieldStr()

	groupStr := q.makeGroupStr()

	limitStr := q.makeLimitStr()

	rs = strings.Trim(fmt.Sprintf("SELECT %s FROM `%s` %s %s %s %s", fieldStr, q.Table, whereStr, groupStr, orderStr, limitStr), " ")
	return
}
