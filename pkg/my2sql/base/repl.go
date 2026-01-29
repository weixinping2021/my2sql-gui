package base

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	//"github.com/siddontang/go-log/log"
)

func ParserAllBinEventsFromRepl(cfg *ConfCmd) {
	defer cfg.CloseChan()

	/*
		if cfg.StartFile == "" {
			files, err := getBinlogFiles(cfg)
			if err != nil {
				log.Fatalf("无法获取 Binlog 列表: %v", err)
			}
			startFile := findStartFile(cfg, files)
			cfg.StartFile = startFile
		}*/
	files, err := getBinlogFiles(cfg)
	if err != nil {
		log.Fatalf("无法获取 Binlog 列表: %v", err)
	}
	startFile := findStartFile(cfg, files)
	cfg.StartFile = startFile
	cfg.BinlogStreamer = NewReplBinlogStreamer(cfg)
	log.Println("start to get binlog from mysql")
	SendBinlogEventRepl(cfg)
	log.Println("finish getting binlog from mysql")
}

func NewReplBinlogStreamer(cfg *ConfCmd) *replication.BinlogStreamer {
	replCfg := replication.BinlogSyncerConfig{
		ServerID:                uint32(cfg.ServerId),
		Flavor:                  cfg.MysqlType,
		Host:                    cfg.Host,
		Port:                    uint16(cfg.Port),
		User:                    cfg.User,
		Password:                cfg.Passwd,
		Charset:                 "utf8",
		SemiSyncEnabled:         false,
		TimestampStringLocation: GBinlogTimeLocation,
		ParseTime:               false, //donot parse mysql datetime/time column into go time structure, take it as string
		UseDecimal:              false, // sqlbuilder not support decimal type
	}

	replSyncer := replication.NewBinlogSyncer(replCfg)

	syncPosition := mysql.Position{Name: cfg.StartFile, Pos: uint32(cfg.StartPos)}
	replStreamer, err := replSyncer.StartSync(syncPosition)
	if err != nil {
		log.Fatalf(fmt.Sprintf("error replication from master %s:%d %v", cfg.Host, cfg.Port, err))
	}
	return replStreamer
}

func SendBinlogEventRepl(cfg *ConfCmd) {
	var (
		err           error
		ev            *replication.BinlogEvent
		chkRe         int
		currentBinlog string = cfg.StartFile
		binEventIdx   uint64 = 0
		trxIndex      uint64 = 0
		trxStatus     int    = 0
		sqlLower      string = ""

		db      string = ""
		tb      string = ""
		sql     string = ""
		sqlType string = ""
		rowCnt  uint32 = 0

		tbMapPos uint32 = 0

		//justStart   bool = true
		//orgSqlEvent *replication.RowsQueryEvent
	)
	for {
		if cfg.IsStopped {
			log.Println("停止解析")
			break
		}
		if cfg.OutputToScreen {
			ev, err = cfg.BinlogStreamer.GetEvent(context.Background())
			if err != nil {
				log.Fatalf(fmt.Sprintf("error to get binlog event"))
				break
			}
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), EventTimeout)
			ev, err = cfg.BinlogStreamer.GetEvent(ctx)
			cancel()
			if err == context.Canceled {
				log.Println("ready to quit! [%v]", err)
				break
			} else if err == context.DeadlineExceeded {
				log.Println("deadline exceeded.")
				break
			} else if err != nil {
				log.Fatalf(fmt.Sprintf("error to get binlog event %v", err))
				break
			}
		}

		if ev.Header.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = ev.Header.LogPos - ev.Header.EventSize
			// avoid mysqlbing mask the row event as unknown table row event
		}
		ev.RawData = []byte{} // we donnot need raw data

		oneMyEvent := &MyBinEvent{MyPos: mysql.Position{Name: currentBinlog, Pos: ev.Header.LogPos}, StartPos: tbMapPos}
		chkRe = oneMyEvent.CheckBinEvent(cfg, ev, &currentBinlog)

		if chkRe == C_reContinue {
			continue
		} else if chkRe == C_reBreak {
			break
		} else if chkRe == C_reFileEnd {
			continue
		}

		db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(ev)
		//if find := strings.Contains(db, "#"); find {
		//	log.Fatalf(fmt.Sprintf("Unsupported database name %s contains special character '#'", db))
		//	break
		//}
		//if find := strings.Contains(tb, "#"); find {
		//	log.Fatalf(fmt.Sprintf("Unsupported table name %s.%s contains special character '#'", db, tb))
		//	break
		//}

		if sqlType == "query" {
			sqlLower = strings.ToLower(sql)
			if sqlLower == "begin" {
				trxStatus = C_trxBegin
				trxIndex++
			} else if sqlLower == "commit" {
				trxStatus = C_trxCommit
			} else if sqlLower == "rollback" {
				trxStatus = C_trxRollback
			} else if oneMyEvent.QuerySql != nil {
				trxStatus = C_trxProcess
				rowCnt = 1
			}

		} else {
			trxStatus = C_trxProcess
		}

		if cfg.WorkType != "stats" {
			ifSendEvent := false
			if cfg.PrintDDL && sqlType == "query" && isDDLKeyword(sql) {
				oneMyEvent.OrgSql = sql
				ifSendEvent = true
			}
			if !cfg.PrintDDL && oneMyEvent.IfRowsEvent {
				tbKey := GetAbsTableName(string(oneMyEvent.BinEvent.Table.Schema),
					string(oneMyEvent.BinEvent.Table.Table))
				_, err = G_TablesColumnsInfo.GetTableInfoJson(string(oneMyEvent.BinEvent.Table.Schema),
					string(oneMyEvent.BinEvent.Table.Table))
				if err != nil {
					log.Fatalf(fmt.Sprintf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s",
						tbKey, oneMyEvent.MyPos.String()))
				}
				ifSendEvent = true
			}
			if ifSendEvent {
				binEventIdx++
				oneMyEvent.EventIdx = binEventIdx
				oneMyEvent.SqlType = sqlType
				oneMyEvent.Timestamp = ev.Header.Timestamp
				oneMyEvent.TrxIndex = trxIndex
				oneMyEvent.TrxStatus = trxStatus
				cfg.EventChan <- *oneMyEvent
			}
		}

		//output analysis result whatever the WorkType is
		if sqlType != "" {
			if sqlType == "query" {
				cfg.StatChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: currentBinlog, StartPos: ev.Header.LogPos - ev.Header.EventSize, StopPos: ev.Header.LogPos,
					Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
			} else {
				cfg.StatChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: currentBinlog, StartPos: tbMapPos, StopPos: ev.Header.LogPos,
					Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
			}
		}

	}
}

// 获取数据库所有 binlog 文件名列表
func getBinlogFiles(cfg *ConfCmd) ([]string, error) {

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql", cfg.User, cfg.Passwd, cfg.Host, cfg.Port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	var files []string
	for rows.Next() {
		// 创建一个对应列数的数组
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 动态扫描
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// 第一个字段 [0] 永远是 Log_name
		if val, ok := values[0].([]byte); ok {
			files = append(files, string(val))
		} else if val, ok := values[0].(string); ok {
			files = append(files, val)
		}
	}
	return files, nil
}

// 获取单个 binlog 文件的起始时间
func getFileStartTime(cfg *ConfCmd, filename string) time.Time {
	cfg_single := replication.BinlogSyncerConfig{
		ServerID: uint32(time.Now().UnixNano()%10000) + 2000,
		Host:     cfg.Host, Port: uint16(cfg.Port), User: cfg.User, Password: cfg.Passwd, Flavor: "mysql",
	}
	syncer := replication.NewBinlogSyncer(cfg_single)
	defer syncer.Close()

	streamer, err := syncer.StartSync(mysql.Position{Name: filename, Pos: 4})
	if err != nil {
		return time.Time{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. 获取第一个事件 (通常是 RotateEvent 或 FormatDescription)
	ev, err := streamer.GetEvent(ctx)
	if err != nil {
		return time.Time{}
	}

	// 2. 获取第二个事件 (真实的业务开始时间)
	ev, err = streamer.GetEvent(ctx)
	if err != nil {
		return time.Time{}
	}

	return time.Unix(int64(ev.Header.Timestamp), 0)
}

func findStartFile(cfg *ConfCmd, files []string) string {

	if len(files) == 0 {
		return ""
	}

	if len(files) == 1 {
		return files[0]
	}

	targetTime := time.Unix(int64(cfg.StartDatetime), 0)
	low, high := 0, len(files)-1
	resultIndex := 0 // 默认从第一个开始

	for low <= high {
		mid := (low + high) / 2
		midTime := getFileStartTime(cfg, files[mid])

		// 如果中间文件的时间早于目标时间，说明起点可能在后面，也可能就是当前这个
		if midTime.Before(targetTime) {
			resultIndex = mid
			low = mid + 1
		} else {
			// 如果中间文件时间已经比目标晚了，起点一定在前面
			high = mid - 1
		}
	}

	// 防御：如果 targetTime 甚至比第一个文件还早，resultIndex 会是 0
	return files[resultIndex]
}
