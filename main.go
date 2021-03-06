package main

import (
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/syyongx/php2go"
	"mysqldump/common"
	xlog "mysqldump/xlog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const Pattern = `\w+:\w+@[\w.]+:\d{0,5}$`

var (
	engine                                                                                                        *xorm.Engine
	flagChunksize, flagThreads, flagPort, flagStmtSize, flagDelete, flagZip                                       int
	flagUser, flagPasswd, flagHost, flagSource, flagDb, flagOutputDir, flagInputDir, flagTable, flagWhere, flagPk, flagZipName, flagUri string

	log = xlog.NewStdLog(xlog.Level(xlog.INFO))
)

func init() {
	flag.StringVar(&flagUser, "u", "", "Username with privileges to run the dump")
	flag.StringVar(&flagPasswd, "p", "", "User password")
	flag.StringVar(&flagHost, "h", "", "The host to connect to")
	flag.IntVar(&flagPort, "P", 3306, "TCP/IP port to connect to")
	flag.StringVar(&flagDb, "db", "", "Database to dump or database to import")
	flag.StringVar(&flagOutputDir, "o", "", "Directory to output files to")
	flag.StringVar(&flagInputDir, "i", "", "Directory of the dump to import")
	flag.IntVar(&flagChunksize, "F", 512, "Split tables into chunks of this output file size. This value is in MB")
	flag.IntVar(&flagThreads, "t", 16, "Number of threads to use")
	flag.IntVar(&flagStmtSize, "s", 1000000, "Attempted size of INSERT statement in bytes")
	flag.StringVar(&flagSource, "m", "", "Mysql source info in one string, format: user:password@host:port")
	flag.StringVar(&flagTable, "table", "", "Mysql table use ',' to split multiple table")
	flag.StringVar(&flagWhere, "where", "", "created > '2020-09-01'")
	flag.StringVar(&flagPk, "pk", "id", "id")
	flag.IntVar(&flagDelete, "D", 0, "1删除 0不删")
	flag.IntVar(&flagZip, "z", 0, "1压缩 0不压缩")
	flag.StringVar(&flagZipName, "zn", "", "zip name")
	flag.StringVar(&flagUri, "uri", "", "完成后通知地址 http://127.0.0.1")

	flag.Usage = usage
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -db [DATABASE] -o [OUTDIR] -i [INDIR] -m [MYSQL_SOURCE] -table [table]")
	flag.PrintDefaults()
	os.Exit(0)
}

func splitSource(input string) (string, string, string, int) {
	sourceSlice := strings.Split(input, "@")
	userSlice := strings.Split(sourceSlice[0], ":")
	addressSlice := strings.Split(sourceSlice[1], ":")
	port, _ := strconv.Atoi(addressSlice[1])
	return userSlice[0], userSlice[1], addressSlice[0], port
}

func createDatabase(path string) {
	if flagDb == "" {
		data, err := common.ReadFile(path + "/dbname")
		common.AssertNil(err)
		flagDb = common.BytesToString(data)
	}
	engine, _ = xorm.NewEngine("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		flagUser, flagPasswd, flagHost, flagPort))
	_, err := engine.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", flagDb))
	common.AssertNil(err)
	log.Info("restoring.database[%s]", flagDb)
}

func generateArgs() *common.Args {
	var flagDir string

	if flagSource != "" {
		if check, _ := regexp.Match(Pattern, []byte(flagSource)); check {
			flagUser, flagPasswd, flagHost, flagPort = splitSource(flagSource)
		} else {
			fmt.Printf("%s can't match regex 'user:password@host:port'", flagSource)
			os.Exit(0)
		}
	}

	if flagHost == "" || flagUser == "" {
		usage()
		os.Exit(0)
	}

	if flagInputDir != "" && flagOutputDir != "" {
		fmt.Println("can't use '-i' and '-o' flag at the same time!")
		os.Exit(0)
	} else if flagInputDir == "" && flagOutputDir == "" {
		fmt.Println("must have flag '-i' or '-o'!")
		os.Exit(0)
	}

	if flagOutputDir != "" {
		if flagDb == "" {
			fmt.Println("must have flag '-db' to special database to dump ")
			os.Exit(0)
		}
		if _, err := os.Stat(flagOutputDir); os.IsNotExist(err) {
			x := os.MkdirAll(flagOutputDir, 0777)
			common.AssertNil(x)
		}
		flagDir = flagOutputDir
	} else {
		flagDir = flagInputDir
		createDatabase(flagDir)
	}

	args := &common.Args{
		Database:      flagDb,
		Outdir:        flagDir,
		ChunksizeInMB: flagChunksize,
		Threads:       flagThreads,
		StmtSize:      flagStmtSize,
		IntervalMs:    10 * 1000,
		Table:         flagTable,
		Where:         flagWhere,
		Pk:            flagPk,
		Delete:        flagDelete,
	}

	return args
}

func main() {
	flag.Parse()
	args := generateArgs()
	engine, _ = xorm.NewEngine("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8",
		flagUser, flagPasswd, flagHost, flagPort, flagDb))
	if flagOutputDir != "" {
		Dumper(log, args, engine)
		if flagZip == 1 {
			err := Zip(flagZipName, args.Outdir)
			if err != nil {
				log.Println(err)
				os.Exit(1)
			}
		}

		if flagUri != "" {
			if php2go.FileExists(flagZipName) {
				md5, err := php2go.Md5File(flagZipName)
				if err != nil {
					log.Println(err)
					os.Exit(1)
				}
				for i := 0; i < 10; i++{
					err := Notify(flagUri, flagZipName, flagZip, args.Outdir, md5)
					if err != nil {
						log.Println(err)
						time.Sleep(time.Second * (time.Duration(i) + 2))
						continue
					} else {
						break
					}
				}
			} else {
				log.Println("压缩文件不存在")
			}
		}

	} else {
		if flagZip == 1 {
			log.Println("start--1")
			if php2go.FileExists(flagZipName) {
				err := UnZip(args.Outdir, flagZipName)
				if err != nil {
					log.Fatal(err.Error())
				}
			}
		}
		log.Println("start")
		Loader(log, args, engine)
		err := os.RemoveAll(args.Outdir)
		log.Println("删除", args.Outdir, err)
	}
}

