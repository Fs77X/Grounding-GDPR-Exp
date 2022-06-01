package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"
	"strconv"
	"encoding/json"
	"os"
	_ "github.com/lib/pq"
	// "io/ioutil"
	"mime/multipart"
	"bytes"
)

// import (

// 	"math/rand"
// 	"strconv"

// )

const (
	DB_USER     = "postgres"
	DB_PASSWORD = "admin"
	DB_NAME     = "the_db"
)

const (
	SIEVE_USER = "sieve"
	SIEVE_PASSWORD = ""
	SIEVE_NAME = "sieve"
)

// type struct ttlRet {
// 	Id string `json:"id"`
// 	TTL int64 `json:"ttl"`
// }

// get id/ttls
// compare with time
// add id to different list for deletions
// delete it

type md struct {
	Id  string `json:"id"`
	TTL int64  `json:"ttl"`
}

type mds struct {
	Key  string `json:"key"`
	TTL int64  `json:"ttl"`
	Device_Id int `json:"device_id"`
}

type mde struct {
	Id  string `json:"id"`
	TTL string  `json:"ttl"`
}

type listResp struct {
	ListMD []md `json:"listMd"`
}

func MDColUT(colname string, newMD *md) interface{} {
	switch colname {
	case "id":
		return &newMD.Id
	case "ttl":
		return &newMD.TTL
	default:
		panic("unknown column " + colname)
	}
}

func MDColUTe(colname string, newMD *mde) interface{} {
	switch colname {
	case "id":
		return &newMD.Id
	case "ttl":
		return &newMD.TTL
	default:
		panic("unknown column " + colname)
	}
}

func MDColUTs(colname string, newMD *mds) interface{} {
	switch colname {
	case "key":
		return &newMD.Key
	case "ttl":
		return &newMD.TTL
	case "device_id":
		return &newMD.Device_Id
	default:
		panic("unknown column " + colname)
	}
}


func sieveTTLID(db *sql.DB) []mds{
	rows, err := db.Query("SELECT key, ttl, device_id from user_policy;")
	checkErr(err)
	var res []mds
	var columns []string
	columns, err1 := rows.Columns()
	checkErr(err1)
	colNum := len(columns)
	for rows.Next() {
		var newMds mds
		cols := make([]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			cols[i] = MDColUTs(columns[i], &newMds)
		}

		err2 := rows.Scan(cols...)
		checkErr(err2)
		res = append(res, newMds)
	}
	return res
}

func getTTLID(db *sql.DB) []md {
	rows, err := db.Query("SELECT id, ttl from usertable;")
	checkErr(err)
	var res []md
	var columns []string
	columns, err1 := rows.Columns()
	checkErr(err1)
	colNum := len(columns)
	for rows.Next() {
		var newMd md
		cols := make([]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			cols[i] = MDColUT(columns[i], &newMd)
		}

		err2 := rows.Scan(cols...)
		checkErr(err2)
		res = append(res, newMd)
	}
	return res
}

func getTTLIDTomb(db *sql.DB) []md {
	rows, err := db.Query("SELECT id, ttl from user_policy where tomb = 0;")
	checkErr(err)
	var res []md
	var columns []string
	columns, err1 := rows.Columns()
	checkErr(err1)
	colNum := len(columns)
	for rows.Next() {
		var newMd md
		cols := make([]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			cols[i] = MDColUT(columns[i], &newMd)
		}

		err2 := rows.Scan(cols...)
		checkErr(err2)
		res = append(res, newMd)
	}
	return res
}

func getTTLIDEnc(db *sql.DB) []mde {
	rows, err := db.Query("SELECT id, PGP_SYM_DECRYPT(ttl::bytea, " + "'key'" + ") as ttl from usertable")
	checkErr(err)
	var res []mde
	var columns []string
	columns, err1 := rows.Columns()
	checkErr(err1)
	colNum := len(columns)
	for rows.Next() {
		var newMd mde
		cols := make([]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			cols[i] = MDColUTe(columns[i], &newMd)
		}

		err2 := rows.Scan(cols...)
		checkErr(err2)
		res = append(res, newMd)
	}
	return res
}

func delTTL(db *sql.DB, listIDTTL []md, vac bool, vacfull bool) {
	currTime := time.Now().Unix()
	for _, mdObj := range listIDTTL {
		if mdObj.TTL < currTime {
			_, err := db.Exec("DELETE FROM usertable where id = $1", mdObj.Id)
			checkErr(err)
			query := "DELETE FROM usertable where id = " + mdObj.Id
			sendLog(query, "del succ")
			if vac {
				_, err1 := db.Exec("VACUUM usertable")
				checkErr(err1)
				query = "VACUUM usertable"
				sendLog(query, "del succ")
			}
			if vacfull {
				_, err1 := db.Exec("VACUUM FULL usertable")
				checkErr(err1)
				query = "VACUUM FULL usertable"
				sendLog(query, "del succ")
			}
		}
		
	}
	fmt.Println("DELETE")
}

func delTTLenc(db *sql.DB, listIDTTL []mde, vac bool) {
	currTime := time.Now().Unix()
	for _, mdObj := range listIDTTL {
		currObjTTL, err0 := strconv.ParseInt(mdObj.TTL, 10, 64)
		checkErr(err0)
		if currObjTTL < currTime {
			_, err := db.Exec("DELETE FROM usertable where id = $1", mdObj.Id)
			checkErr(err)
			if vac {
				_, err1 := db.Exec("VACUUM FULL usertable")
				checkErr(err1)
			}
		}
	}
	fmt.Println("DELETE")
}

func sendLog(query string, result string) {
	url := "http://localhost:8000/add_log/"
	method := "POST"
  
	payload := &bytes.Buffer{}
	writer := multipart.NewWriter(payload)
	_ = writer.WriteField("querier", "ttldaemon")
	_ = writer.WriteField("query", query)
	_ = writer.WriteField("result", result)
	err := writer.Close()
	checkErr(err)
	client := &http.Client {}
	req, err1 := http.NewRequest(method, url, payload)
	checkErr(err1)
	req.Header.Set("Content-Type", writer.FormDataContentType())
  	res, err2 := client.Do(req)
	checkErr(err2)
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		fmt.Println(res.StatusCode)
		fmt.Println("ISSUE")
	}
}

func delTombstone(db *sql.DB, listIDTTL []md) {
	currTime := time.Now().Unix()
	for _, mdObj := range listIDTTL {
		// fmt.Println(mdObj.TTL)
		if mdObj.TTL < currTime {
			// fmt.Println("UPDATE usertable set tomb = 1 where id = " + mdObj.Id)
			query := "UPDATE user_policy set tomb = 1 where id = $1"
			_, err := db.Exec(query, mdObj.Id)
			checkErr(err)
			query ="UPDATE user_policy set tomb = 1 where id = " + mdObj.Id
			sendLog(query, "del succ")
		}
	}
	fmt.Println("DELETETOMB")
}

func delSeppy(db *sql.DB, listIDTTL []md) {
	currTime := time.Now().Unix()
	for _, mdObj := range listIDTTL {
		// fmt.Println(mdObj.TTL)
		if mdObj.TTL < currTime {
			_, err := db.Exec("DELETE FROM mall_observation where id = $1", mdObj.Id)
			checkErr(err)
			_, err1 := db.Exec("DELETE FROM user_policy where id = $1", mdObj.Id)
			checkErr(err1)

		}
	}
	fmt.Println("DELSEPPY")

}

func perfSieveDel(device_id int, id string) {
	devid := strconv.Itoa(device_id)
	url := "http://localhost:5344/sieve/mdelete_obj/" +  devid + "/" + id
	method := "DELETE"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
		return
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		fmt.Println("NO BUENO")
	}
}

func delSieve(listIDTTL []mds) {
	currTime := time.Now().Unix()
	for _, mdObj := range listIDTTL {
		if mdObj.TTL < currTime {
			perfSieveDel(mdObj.Device_Id, mdObj.Key)
		}
	}
	fmt.Println("DELSIEVE")
}


func delLSMeta(key string) {
	url := "http://localhost:8000/mdelete_UserMetaobj/" + key
	method := "DELETE"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
		return
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		fmt.Println("NO BUENO")
	}
}

func delLSData(key string) {
	url := "http://localhost:8000/mdelete_obj/" + key
	method := "DELETE"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
		return
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		fmt.Println("NO BUENO")
	}
}

func gdprLSTTL() {
	url := "http://localhost:8080/getTTL"
	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		fmt.Println(err)
		return
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()
	var data []md
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		checkErr(err)
	}
	// body, err := ioutil.ReadAll(res.Body)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	currTime := time.Now().Unix()
	var delIds []string
	for i := 0; i < len(data); i++ {
		if data[i].TTL < currTime {
			delIds = append(delIds, data[i].Id)
		}
	}

	for i := 0; i < len(delIds); i++ {
		delLSData(delIds[i])
		delLSMeta(delIds[i])
	}

}

// basically same step with tombstoning
func main() {
	var db *sql.DB
	tick := time.Tick(5 * time.Minute)
	if os.Args[1] == "p2" {
		db = setupSieve()
	} else {
		db = setupDB()
	}
	
	vac := false
	vacfull := false
	if os.Args[1] == "vac" {
		vac = true
	}
	if os.Args[1] == "vacfull" {
		vacfull = true
	}
	for range tick {
		fmt.Println("Tick")
		if os.Args[1] == "p1" {
			listIDTTL := getTTLID(db)
			delTTL(db, listIDTTL, true, false)
		} else if os.Args[1] == "p2" {
			listIDTTL := sieveTTLID(db)
			delSieve(listIDTTL)
		} else if os.Args[1] == "p3" {
			listIDTTL := getTTLIDTomb(db)
			delTombstone(db, listIDTTL)
		} else {
			listIDTTL := getTTLID(db)
			delTTL(db, listIDTTL, vac, vacfull)
		}
		// listIDTTL := getTTLIDTomb(db)
		// listIDTTL := getTTLIDEnc(db)
		// gdprLSTTL()
		// delTombstone(db, listIDTTL)
		// delTTL(db, listIDTTL, vac, vacfull)
		// delTTLenc(db, listIDTTL, true)
		// delSeppy(db, listIDTTL)
		// time.Sleep(5 * time.Second)
	}
}

func setupDB() *sql.DB {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)

	checkErr(err)

	return db
}

func setupSieve() *sql.DB {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", SIEVE_USER, SIEVE_PASSWORD, SIEVE_NAME)
	db, err := sql.Open("postgres", dbinfo)

	checkErr(err)

	return db
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}
