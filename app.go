package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	my "my-wails-app/pkg/my2sql/base"
	"my-wails-app/pkg/my2sql/constvar"

	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

type App struct {
	ctx context.Context
}

// 定义一个日志桥接器
type LogBridge struct {
	a *App
}

// 实现 io.Writer 接口的 Write 方法
func (lb *LogBridge) Write(p []byte) (n int, err error) {
	msg := string(p)
	// 依然在终端打印，方便调试
	os.Stdout.Write(p)
	// 实时推送到前端
	if lb.a.ctx != nil {
		runtime.EventsEmit(lb.a.ctx, "backend-log", msg)
	}
	return len(p), nil
}

func NewApp() *App {
	return &App{}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	log.SetOutput(&LogBridge{a: a})
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

// ParseConnectionString 解析连接字符串
func ParseConnectionString(connStr string) (string, error) {
	// 将 root:password@tcp(127.0.0.1:3306)
	// 转换为 root:password@tcp(127.0.0.1:3306)/
	if !strings.HasSuffix(connStr, "/") {
		connStr += "/"
	}
	return connStr, nil
}

// TestConnection 测试连接并获取数据库列表
func (a *App) TestConnection(connStr string) ([]string, error) {
	dsn, err := ParseConnectionString(connStr)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %v", err)
	}
	defer db.Close()

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping 失败: %v", err)
	}

	// 获取数据库列表
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("获取数据库列表失败: %v", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			continue
		}
		// 过滤系统数据库
		if dbName != "information_schema" && dbName != "mysql" &&
			dbName != "performance_schema" && dbName != "sys" {
			databases = append(databases, dbName)
		}
	}

	return databases, nil
}

// GetTables 获取指定数据库的表列表
func (a *App) GetTables(connStr string, databases []string) ([]string, error) {
	if len(databases) == 0 {
		return []string{}, nil
	}

	dsn, err := ParseConnectionString(connStr)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 构建 IN 查询
	placeholders := make([]string, len(databases))
	args := make([]interface{}, len(databases))
	for i, dbName := range databases {
		placeholders[i] = "?"
		args[i] = dbName
	}

	query := fmt.Sprintf(`
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA IN (%s)
        ORDER BY TABLE_NAME
    `, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	seen := make(map[string]bool) // 去重
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		if !seen[tableName] {
			tables = append(tables, tableName)
			seen[tableName] = true
		}
	}

	return tables, nil
}

type BinlogResult struct {
	ID        int    `json:"id"`
	Operation string `json:"operation"`
	Database  string `json:"database"`
	Table     string `json:"table"`
	Records   int    `json:"records"`
	Timestamp string `json:"timestamp"`
}

// 增加全局取消函数

func (a *App) StopAnalyze() {
	my.GConfCmd.IsStopped = true
}

// AnalyzeRequest 对应前端 form 提交的所有字段
type AnalyzeRequest struct {
	ConnectionString string   `json:"connectionString"`
	OutputDir        string   `json:"outputDir"`
	Databases        []string `json:"databases"`
	Tables           []string `json:"tables"`
	Threads          int      `json:"threads"`
	IncludeDDL       bool     `json:"includeDDL"`
	IncludeInsert    bool     `json:"includeInsert"`
	IncludeUpdate    bool     `json:"includeUpdate"`
	IncludeDelete    bool     `json:"includeDelete"`
	WorkType         string   `json:"worktype"`
	// 这两个是我们在前端 onFinish 里处理后的字符串格式时间
	StartDatetime string `json:"startDatetime"`
	StopDatetime  string `json:"stopDatetime"`
}

func (a *App) AnalyzeBinlog(req AnalyzeRequest) error {

	// 解析连接字符串
	connStr := req.ConnectionString
	user, password, host, port, err := parseConnectionString(connStr)
	if err != nil {
		return fmt.Errorf("解析连接字符串失败: %v", err)
	}

	my.GConfCmd.SqlTblPrefixDb = false

	my.GConfCmd.Databases = req.Databases
	my.GConfCmd.Passwd = password
	my.GConfCmd.User = user
	my.GConfCmd.Host = host
	my.GConfCmd.Port = uint(port)
	my.GConfCmd.Tables = req.Tables

	// 设置表过滤
	/*
		if tables, ok := config["tables"].([]interface{}); ok && len(tables) > 0 {
			tableList := make([]string, len(tables))
			for i, table := range tables {
				tableList[i] = table.(string)
			}
			my.GConfCmd.Tables = tableList
		}
	*/

	// 设置操作类型
	if req.IncludeDDL {
		my.GConfCmd.PrintDDL = true
	} else {
		sqlTypes := []string{}
		if req.IncludeInsert {
			sqlTypes = append(sqlTypes, "insert")
		}
		if req.IncludeUpdate {
			sqlTypes = append(sqlTypes, "update")
		}
		if req.IncludeDelete {
			sqlTypes = append(sqlTypes, "delete")
		}
		my.GConfCmd.FilterSql = sqlTypes
		my.GConfCmd.FilterSqlLen = len(sqlTypes)
	}

	GBinlogTimeLocation, err := time.LoadLocation("Local")

	if req.StartDatetime != "" {
		t, err := time.ParseInLocation(constvar.DATETIME_FORMAT, req.StartDatetime, GBinlogTimeLocation)
		if err != nil {
			log.Println(err.Error())
		}
		my.GConfCmd.StartDatetime = uint32(t.Unix())
		my.GConfCmd.IfSetStartDateTime = true
	} else {
		my.GConfCmd.IfSetStartDateTime = false
	}

	if req.StopDatetime != "" {
		t, err := time.ParseInLocation(constvar.DATETIME_FORMAT, req.StopDatetime, GBinlogTimeLocation)
		if err != nil {
			log.Fatalf("invalid stop datetime -stop-datetime " + req.StopDatetime)
		}
		my.GConfCmd.StopDatetime = uint32(t.Unix())
		my.GConfCmd.IfSetStopDateTime = true
	} else {
		my.GConfCmd.IfSetStopDateTime = false
	}

	if req.StopDatetime != "" && req.StartDatetime != "" {
		if my.GConfCmd.StartDatetime >= my.GConfCmd.StopDatetime {
			log.Fatalf("-start-datetime must be ealier than -stop-datetime")
		}
	}

	my.GConfCmd.OutputDir = req.OutputDir
	/*
		if this.StartFile != "" {
			this.IfSetStartFilePos = true
			this.StartFilePos = mysql.Position{Name: this.StartFile, Pos: uint32(this.StartPos)}

		} else {
			this.IfSetStartFilePos = false
		}
	*/
	my.GConfCmd.IfSetStartFilePos = false

	/*
		if this.StopFile != "" {
			this.IfSetStopFilePos = true
			this.StopFilePos = mysql.Position{Name: this.StopFile, Pos: uint32(this.StopPos)}
			this.IfSetStopParsPoint = true

		} else {
			this.IfSetStopFilePos = false
			this.IfSetStopParsPoint = false
		}
	*/
	my.GConfCmd.IfSetStopFilePos = false
	my.GConfCmd.IfSetStopParsPoint = false

	/*
		if this.Mode == "file" {

			if this.StartFile == "" {
				log.Fatalf("missing binlog file.  -start-file must be specify when -mode=file ")
			}
			this.GivenBinlogFile = this.StartFile
			if !toolkits.IsFile(this.GivenBinlogFile) {
				log.Fatalf("%s doesnot exists nor a file\n", this.GivenBinlogFile)
			} else {
				this.BinlogDir = filepath.Dir(this.GivenBinlogFile)
			}
		}
		if my.GConfCmd.Mode == "file" {
			if my.GConfCmd.LocalBinFile == "" {
				log.Fatalf("missing binlog file.  -local-binlog-file must be specify when -mode=file ")
			}
			my.GConfCmd.GivenBinlogFile = my.GConfCmd.LocalBinFile
			if !toolkits.IsFile(my.GConfCmd.GivenBinlogFile) {
				log.Fatalf("%s doesnot exists nor a file\n", my.GConfCmd.GivenBinlogFile)
			} else {
				my.GConfCmd.BinlogDir = filepath.Dir(my.GConfCmd.GivenBinlogFile)
			}
		}*/
	// 设置线程数
	my.GConfCmd.IsStopped = false
	my.GConfCmd.Threads = uint(req.Threads)
	my.GConfCmd.PrintInterval = my.GConfCmd.GetDefaultValueOfRange("PrintInterval")
	my.GConfCmd.BigTrxRowLimit = my.GConfCmd.GetDefaultValueOfRange("BigTrxRowLimit")
	my.GConfCmd.LongTrxSeconds = my.GConfCmd.GetDefaultValueOfRange("LongTrxSeconds")
	my.GConfCmd.ServerId = 1113306
	my.GConfCmd.Mode = "repl"
	my.GConfCmd.WorkType = req.WorkType
	my.GConfCmd.MysqlType = "mysql"
	my.GConfCmd.PrintExtraInfo = true
	my.GConfCmd.EventChan = make(chan my.MyBinEvent, my.GConfCmd.Threads*2)
	my.GConfCmd.StatChan = make(chan my.BinEventStats, my.GConfCmd.Threads*2)
	my.GConfCmd.SqlChan = make(chan my.ForwardRollbackSqlOfPrint, my.GConfCmd.Threads*2)
	my.GConfCmd.StatChan = make(chan my.BinEventStats, my.GConfCmd.Threads*2)
	my.GConfCmd.OpenStatsResultFiles()
	my.GConfCmd.OpenTxResultFiles()

	my.GConfCmd.CheckCmdOptions()
	my.GConfCmd.CreateDB()

	my.GConfCmd.IfSetStopParsPoint = false
	//my.GConfCmd.ParseCmdOptions()
	defer my.GConfCmd.CloseFH()

	if my.GConfCmd.WorkType != "stats" {
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
	}
	var wg, wgGenSql sync.WaitGroup
	wg.Add(1)
	go my.ProcessBinEventStats(my.GConfCmd, &wg)

	if my.GConfCmd.WorkType != "stats" {
		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, &wg)
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, &wgGenSql)
		}
	}
	if my.GConfCmd.Mode == "repl" {
		my.ParserAllBinEventsFromRepl(my.GConfCmd)
	} else if my.GConfCmd.Mode == "file" {
		myParser := my.BinFileParser{}
		myParser.Parser = replication.NewBinlogParser()
		// donot parse mysql datetime/time column into go time structure, take it as string
		myParser.Parser.SetParseTime(false)
		// sqlbuilder not support decimal type
		myParser.Parser.SetUseDecimal(false)
		myParser.MyParseAllBinlogFiles(my.GConfCmd)
	}
	wgGenSql.Wait()
	close(my.GConfCmd.SqlChan)
	wg.Wait()
	return err

}

// ParseBinlogStatus 解析分析产生的 txt 报告
func (a *App) ParseBinlogStatus(filepath string) ([]BinlogResult, error) {
	// 关键：初始化为空切片而不是 nil，防止前端 results.length 报错

	results := make([]BinlogResult, 0)

	file, err := os.Open(filepath)
	if err != nil {
		return results, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// 使用正则表达式匹配一个或多个空格
	re := regexp.MustCompile(`\s+`)
	idCounter := 1

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// 1. 过滤掉空行
		// 2. 过滤掉表头行（你的文件第一行是 binlog starttime...）
		if line == "" {
			continue
		}

		// 2. 精准过滤表头：只有这一行同时包含 binlog 和 starttime 时，才是标题行
		if strings.Contains(line, "binlog") && strings.Contains(line, "starttime") {
			continue
		}
		// 3. 拆分行数据
		parts := re.Split(line, -1)

		// 根据你的文件内容预览：
		// 索引 0: mysql-bin.001092
		// 索引 1: 2026-01-21_16:59:10 (starttime)
		// 索引 5: 1 (inserts)
		// 索引 6: 0 (updates)
		// 索引 7: 0 (deletes)
		// 索引 8: dbtest1 (database)
		// 索引 9: test7 (table)

		if len(parts) < 10 {
			continue
		}

		timestamp := strings.Replace(parts[1], "_", " ", 1)
		inserts, _ := strconv.Atoi(parts[5])
		updates, _ := strconv.Atoi(parts[6])
		deletes, _ := strconv.Atoi(parts[7])
		dbName := parts[8]
		tableName := parts[9]

		// 只有数量大于 0 的才返回，这样前端表格才不会有一堆 0 记录的行
		if inserts > 0 {
			results = append(results, BinlogResult{
				ID: idCounter, Operation: "INSERT", Database: dbName, Table: tableName, Records: inserts, Timestamp: timestamp,
			})
			idCounter++
		}
		if updates > 0 {
			results = append(results, BinlogResult{
				ID: idCounter, Operation: "UPDATE", Database: dbName, Table: tableName, Records: updates, Timestamp: timestamp,
			})
			idCounter++
		}
		if deletes > 0 {
			results = append(results, BinlogResult{
				ID: idCounter, Operation: "DELETE", Database: dbName, Table: tableName, Records: deletes, Timestamp: timestamp,
			})
			idCounter++
		}
	}

	return results, scanner.Err()
}

// parseConnectionString 解析连接字符串
// 输入: root:password@tcp(127.0.0.1:3306)
// 输出: user, password, host, port, error
func parseConnectionString(connStr string) (string, string, string, int, error) {
	// 移除 @tcp( 和 )
	connStr = strings.TrimSpace(connStr)

	// 分离用户信息和地址
	parts := strings.Split(connStr, "@tcp(")
	if len(parts) != 2 {
		return "", "", "", 0, fmt.Errorf("无效的连接字符串格式")
	}

	// 解析用户名和密码
	userPass := parts[0]
	userParts := strings.Split(userPass, ":")
	if len(userParts) != 2 {
		return "", "", "", 0, fmt.Errorf("无效的用户名密码格式")
	}
	user := userParts[0]
	password := userParts[1]

	// 解析主机和端口
	hostPort := strings.TrimSuffix(parts[1], ")")
	hostParts := strings.Split(hostPort, ":")
	if len(hostParts) != 2 {
		return "", "", "", 0, fmt.Errorf("无效的主机端口格式")
	}
	host := hostParts[0]
	port, err := strconv.Atoi(hostParts[1])
	if err != nil {
		return "", "", "", 0, fmt.Errorf("无效的端口号")
	}

	return user, password, host, port, nil
}

// ExportSQL 导出 SQL
func (a *App) ExportSQL(config map[string]interface{}, exportType string) (string, error) {
	my.GConfCmd.IfSetStopParsPoint = false
	my.GConfCmd.ParseCmdOptions()
	defer my.GConfCmd.CloseFH()
	if my.GConfCmd.WorkType != "stats" {
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
	}
	var wg, wgGenSql sync.WaitGroup
	wg.Add(1)
	go my.ProcessBinEventStats(my.GConfCmd, &wg)

	if my.GConfCmd.WorkType != "stats" {
		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, &wg)
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, &wgGenSql)
		}
	}
	if my.GConfCmd.Mode == "repl" {
		my.ParserAllBinEventsFromRepl(my.GConfCmd)
	} else if my.GConfCmd.Mode == "file" {
		myParser := my.BinFileParser{}
		myParser.Parser = replication.NewBinlogParser()
		// donot parse mysql datetime/time column into go time structure, take it as string
		myParser.Parser.SetParseTime(false)
		// sqlbuilder not support decimal type
		myParser.Parser.SetUseDecimal(false)
		myParser.MyParseAllBinlogFiles(my.GConfCmd)
	}
	wgGenSql.Wait()
	close(my.GConfCmd.SqlChan)
	wg.Wait()
	// exportType: "forward" 或 "rollback"

	// TODO: 调用 my2sql 生成 SQL 文件
	// my2sql --work-type 2sql --mode repl ...
	// 或 --work-type rollback

	outputFile := fmt.Sprintf("%s_%s.sql", exportType, time.Now().Format("20060102_150405"))

	return fmt.Sprintf("已导出到: %s", outputFile), nil
}

// SelectFolder 唤起原生对话框选择结果存放路径
func (a *App) SelectFolder() (string, error) {
	directory, err := runtime.OpenDirectoryDialog(a.ctx, runtime.OpenDialogOptions{
		Title: "选择结果存放目录",
	})
	if err != nil {
		return "", err
	}
	return directory, nil
}
