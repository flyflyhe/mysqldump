package main

import (
	"fmt"
	"github.com/go-xorm/xorm"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"xorm.io/core"

	"mysqldump/common"
	xlog "mysqldump/xlog"
)

func writeDBName(args *common.Args) {
	file := fmt.Sprintf("%s/dbname", args.Outdir)
	_ = common.WriteFile(file, args.Database)
}

func dumpViewSchema(log *xlog.Log, engine *xorm.Engine, args *common.Args) {
	qr, err := engine.QueryString(fmt.Sprintf("SHOW TABLE STATUS FROM %s WHERE Comment='view';", args.Database))
	common.AssertNil(err)

	for _, t := range qr {
		viewName := t["Name"]
		create, err := engine.QueryString(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", args.Database, viewName))
		common.AssertNil(err)

		schema := create[0]["Create View"] + ";\n"
		file := fmt.Sprintf("%s/%s-view.sql", args.Outdir, viewName)
		_ = common.WriteFile(file, schema)
		log.Info("dumping.view[%s.%s].schema...", args.Database, viewName)
	}
}

func dumpRoutineSchema(log *xlog.Log, engine *xorm.Engine, args *common.Args, routineType string) {
	qr, err := engine.QueryString(fmt.Sprintf("SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_TYPE = '%s' AND ROUTINE_SCHEMA = '%s'", routineType, args.Database))
	common.AssertNil(err)

	for _, t := range qr {
		routineName := t["ROUTINE_NAME"]
		create, err := engine.QueryString(fmt.Sprintf("SHOW CREATE %s `%s`.`%s`", routineType, args.Database, routineName))
		common.AssertNil(err)

		schema := create[0][fmt.Sprintf("Create %s", strings.Title(strings.ToLower(routineType)))] + ";\n"
		file := fmt.Sprintf("%s/%s-%s-%s.sql", args.Outdir, routineName, time.Now().Format("20060102") ,strings.ToLower(routineType))
		_ = common.WriteFile(file, schema)
		log.Info("dumping.routine[%s.%s].schema...", args.Database, routineName)
	}
}

func dumpTableSchema(log *xlog.Log, engine *xorm.Engine, args *common.Args, tableName string) {
	qr, err := engine.QueryString(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", args.Database, tableName))
	common.AssertNil(err)
	file := fmt.Sprintf("%s/%s-table.sql", args.Outdir, tableName)
	_ = common.WriteFile(file, qr[0]["Create Table"]+";\n")
	log.Info("dumping.table[%s.%s].schema...", args.Database, tableName)
}

func dumpTable(log *xlog.Log, engine *xorm.Engine, args *common.Args, table *core.Table) {
	var allBytes uint64
	var allRows uint64
	var querySql string
	var startId int64
	var endId int64

	if args.Where != "" {
		querySql = fmt.Sprintf("SELECT /*backup*/ * FROM `%s`.`%s` where %s", args.Database, table.Name, args.Where)
	} else {
		querySql = fmt.Sprintf("SELECT /*backup*/ * FROM `%s`.`%s`", args.Database, table.Name)
	}

	log.Println("query sql : ", querySql)
	cursor, err := engine.DB().Query(querySql)
	common.AssertNil(err)

	cols := table.ColumnsSeq()
	dialect := engine.Dialect()
	destColNames := dialect.Quote(strings.Join(cols, dialect.Quote(", ")))

	fileNo := 1
	stmtsize := 0
	chunkbytes := 0
	rows := make([]string, 0, 256)
	inserts := make([]string, 0, 256)
	for cursor.Next() {
		dest := make([]interface{}, len(cols))
		err = cursor.ScanSlice(&dest)
		common.AssertNil(err)

		var temp string
		for i, d := range dest {
			col := table.GetColumn(cols[i])

			if col.Name == args.Pk {
				var id int64
				switch reflect.TypeOf(d).Kind() {
				case reflect.Slice:
					id, _ = strconv.ParseInt(string(d.([]byte)), 10, 64)
				case reflect.Int16, reflect.Int8, reflect.Int32, reflect.Int64, reflect.Int:
					id = d.(int64)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					id = d.(int64)
				}
				if startId == 0 {
					startId = id
				}
				endId = id
			}

			if d == nil {
				temp += ", NULL"
			} else if col.SQLType.IsText() || col.SQLType.IsTime() {
				var v = fmt.Sprintf("%s", d)
				if strings.HasSuffix(v, " +0000 UTC") {
					temp += fmt.Sprintf(", '%s'", v[0:len(v)-len(" +0000 UTC")])
				} else {
					temp += ", '" + common.EscapeString(v) + "'"
				}
			} else if col.SQLType.IsBlob() {
				if reflect.TypeOf(d).Kind() == reflect.Slice {
					temp += fmt.Sprintf(", %s", dialect.FormatBytes(d.([]byte)))
				} else if reflect.TypeOf(d).Kind() == reflect.String {
					temp += fmt.Sprintf(", '%s'", d.(string))
				}
			} else if col.SQLType.IsNumeric() {
				switch reflect.TypeOf(d).Kind() {
				case reflect.Slice:
					if col.SQLType.Name == core.Bool {
						temp += fmt.Sprintf(", %v", strconv.FormatBool(d.([]byte)[0] != byte('0')))
					} else {
						temp += fmt.Sprintf(", %s", string(d.([]byte)))
					}
				case reflect.Int16, reflect.Int8, reflect.Int32, reflect.Int64, reflect.Int:
					if col.SQLType.Name == core.Bool {
						temp += fmt.Sprintf(", %v", strconv.FormatBool(reflect.ValueOf(d).Int() > 0))
					} else {
						temp += fmt.Sprintf(", %v", d)
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					if col.SQLType.Name == core.Bool {
						temp += fmt.Sprintf(", %v", strconv.FormatBool(reflect.ValueOf(d).Uint() > 0))
					} else {
						temp += fmt.Sprintf(", %v", d)
					}
				default:
					temp += fmt.Sprintf(", %v", d)
				}
			} else {
				s := fmt.Sprintf("%v", d)
				if strings.Contains(s, ":") || strings.Contains(s, "-") {
					if strings.HasSuffix(s, " +0000 UTC") {
						temp += fmt.Sprintf(", '%s'", s[0:len(s)-len(" +0000 UTC")])
					} else {
						temp += fmt.Sprintf(", '%s'", s)
					}
				} else {
					temp += fmt.Sprintf(", %s", s)
				}
			}
		}
		r := "(" + temp[2:] + ")"
		rows = append(rows, r)

		allRows++
		stmtsize += len(r)
		chunkbytes += len(r)
		allBytes += uint64(len(r))
		atomic.AddUint64(&args.Allbytes, uint64(len(r)))
		atomic.AddUint64(&args.Allrows, 1)

		if stmtsize >= args.StmtSize {
			insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table.Name, destColNames, strings.Join(rows, ",\n"))
			inserts = append(inserts, insertone)
			rows = rows[:0]
			stmtsize = 0
		}

		if (chunkbytes / 1024 / 1024) >= args.ChunksizeInMB {
			query := strings.Join(inserts, ";\n") + ";\n"
			file := fmt.Sprintf("%s/%s.%05d.sql", args.Outdir, table.Name, fileNo)
			_ = common.WriteFile(file, query)

			log.Info("dumping.table[%s.%s].rows[%v].bytes[%vMB].part[%v]", args.Database, table.Name, allRows, allBytes/1024/1024, fileNo)
			inserts = inserts[:0]
			chunkbytes = 0
			fileNo++
		}
	}
	if chunkbytes > 0 {
		if len(rows) > 0 {
			insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table.Name, destColNames, strings.Join(rows, ",\n"))
			inserts = append(inserts, insertone)
		}

		query := strings.Join(inserts, ";\n") + ";\n"
		file := fmt.Sprintf("%s/%s.%s.%05d.sql", args.Outdir, time.Now().Format("20060102"), table.Name, fileNo)
		_ = common.WriteFile(file, query)
	}
	err = cursor.Close()
	common.AssertNil(err)

	log.Println("startId", startId, "endId", endId)
	log.Println("delete", args.Delete)
	if args.Delete == 1 {
		Delete(startId, endId, args)
	}
	log.Info("dumping.table[%s.%s].done.allrows[%v].allbytes[%vMB]...", args.Database, table.Name, allRows, allBytes/1024/1024)
}

func Delete(startId, endId int64, args *common.Args)  {
	if startId > endId {
		return
	}

	start := startId
	batch := int64(1000)
	for {
		end := start + batch
		if end > endId {
			end = endId
		}

		deleteSql := fmt.Sprintf("delete from `%s` where  `%s` >= %d and `%s` <= %d", args.Table, args.Pk, start, args.Pk, end)
		log.Println("delete sql:", deleteSql)
		result, err := engine.DB().Exec(deleteSql)
		if err != nil {
			log.Println("delete err:", err)
		} else {
			line, err := result.RowsAffected()
			if err != nil {
				log.Println("rows err:", err)
			} else {
				log.Println("删除行数:", line)
			}
		}

		start = end
		if end >= endId {
			break
		}
	}
}

// Dumper used to start the dumper worker.
func Dumper(log *xlog.Log, args *common.Args, engine *xorm.Engine) {

	var wg sync.WaitGroup
	t := time.Now()

	tables, err := engine.DBMetas()
	common.AssertNil(err)

	//databaseName
	go writeDBName(args)
	//function
	go dumpRoutineSchema(log, engine, args, "FUNCTION")
	//procedure
	go dumpRoutineSchema(log, engine, args, "PROCEDURE")
	//view
	go dumpViewSchema(log, engine, args)

	for _, table := range tables {
		if args.Table == table.Name {
			dumpTableSchema(log, engine, args, table.Name)
		}

		wg.Add(1)
		go func(engine *xorm.Engine, table *core.Table) {
			defer func() {
				wg.Done()
			}()
			// excludeTable can't dump data
			if args.Table == table.Name {
				log.Info("dumping.table[%s.%s].datas...", args.Database, table.Name)
				dumpTable(log, engine, args, table)
				log.Info("dumping.table[%s.%s].datas.done...", args.Database, table.Name)
			}
		}(engine, table)
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(args.IntervalMs))
	defer tick.Stop()
	go func() {
		for range tick.C {
			diff := time.Since(t).Seconds()
			allbytesMB := float64(atomic.LoadUint64(&args.Allbytes) / 1024 / 1024)
			allrows := atomic.LoadUint64(&args.Allrows)
			rates := allbytesMB / diff
			log.Info("dumping.allbytes[%vMB].allrows[%v].time[%.2fsec].rates[%.2fMB/sec]...", allbytesMB, allrows, diff, rates)
		}
	}()
	wg.Wait()
	elapsedStr, elapsed := time.Since(t).String(), time.Since(t).Seconds()
	log.Info("dumping.all.done.cost[%s].allrows[%v].allbytes[%v].rate[%.2fMB/s]", elapsedStr, args.Allrows, args.Allbytes, float64(args.Allbytes/1024/1024)/elapsed)
}
