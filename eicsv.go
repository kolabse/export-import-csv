package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"database/sql"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/joho/sqltocsv"
)

var (
	dResult       []byte
	iResult       []byte
	startTime     time.Time
	dumpStatus    string = "Idle!"
	importStatus  string = "Idle!"
	lastFilename  string
	portNumb      int
	flagTableName string
	flagFileName  string
	flagActoin    string
	flagDbURL     string
)

//DumpSettings struct for pars settings from request
type DumpSettings struct {
	Table string
	Start int
	Total int
}

//ImportSettings struct for pars settings from request
type ImportSettings struct {
	Table string
	File  string
}

//StatusResponse struct
type StatusResponse struct {
	DumpStatus   string
	ImportStatus string
	Uptime       string
}

//SuccessResponse struct
type SuccessResponse struct {
	Table    string
	Message  string
	FileName string
}

func init() {
	startTime = time.Now()
}

func main() {

	flag.IntVar(&portNumb, "port", 8080, "Port number for service")
	flag.StringVar(&flagDbURL, "dburl", "none", "DB connection URL with creditionals")
	flag.StringVar(&flagActoin, "action", "none", "Direction (import/export)")
	flag.StringVar(&flagTableName, "table", "none", "Table name to import/export")
	flag.StringVar(&flagFileName, "file", "none", "File name to export")

	flag.Parse()

	printWelcome()

	if flagDbURL == "none" {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter dbUrl to continue or type 'exit' to close program: ")
		userDbURL, _ := reader.ReadString('\n')
		if userDbURL != "exit\n" {
			flagDbURL = userDbURL
		} else {
			fmt.Print("Bye!")
			os.Exit(0)
		}
	}

	db, err := sql.Open("mysql", flagDbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	switch flagActoin {
	case "import":
		if flagFileName != "none" && flagTableName != "none" {
			importTable(db, flagFileName, flagTableName)
		} else {
			panic("not enough parameters")
		}
	case "export":
		if flagTableName != "none" {
			fileName := getUUID() + ".csv"
			dumpTable(db, fileName, flagTableName)
		} else {
			panic("not enough parameters")
		}
	default:
		log.Printf("Starting service")
		http.HandleFunc("/dump", func(w http.ResponseWriter, r *http.Request) {

			decoder := json.NewDecoder(r.Body)
			var settings DumpSettings
			err := decoder.Decode(&settings)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte((err.Error())))
			}

			log.Printf("Table: %s", settings.Table)
			log.Printf("Start: %d", settings.Start)
			log.Printf("Total: %d", settings.Total)

			fileName := getUUID() + ".csv"

			go dumpTable(db, fileName, settings.Table)

			dResult, _ = json.Marshal(&SuccessResponse{
				Table:    settings.Table,
				Message:  "Table will be dumped",
				FileName: fileName,
			})
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, string(dResult[:]))

		})
		http.HandleFunc("/import", func(w http.ResponseWriter, r *http.Request) {

			decoder := json.NewDecoder(r.Body)
			var settings ImportSettings
			err := decoder.Decode(&settings)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte((err.Error())))
			}

			fileName := settings.File
			tableName := settings.Table

			log.Printf("Table: %s", tableName)
			log.Printf("File: %s", fileName)

			go importTable(db, fileName, tableName)

			iResult, _ = json.Marshal(&SuccessResponse{
				Table:    tableName,
				Message:  "Table will be imported",
				FileName: fileName,
			})
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, string(iResult[:]))

		})
		http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			serviceStatus, _ := json.Marshal(&StatusResponse{
				DumpStatus:   dumpStatus,
				ImportStatus: importStatus,
				Uptime:       shortDur(uptime()),
			})
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, string(serviceStatus[:]))
		})
		log.Fatal(http.ListenAndServe(":"+strconv.Itoa(portNumb), nil))
	}

}

func dumpTable(db *sql.DB, fileName string, tableName string) {
	workStart := time.Now()
	dumpStatus = "Dump in progress!"
	lastFilename = fileName

	log.Printf("Starting export process")

	rows, err := db.Query("SELECT * FROM " + tableName)
	if err != nil {
		log.Printf("We got error: " + err.Error())
		return
	}
	defer rows.Close()

	csvConverter := sqltocsv.New(rows)
	csvConverter.Delimiter = ','

	dbErr := csvConverter.WriteFile(fileName)

	if dbErr != nil {
		panic(err)
	}
	if lastFilename == fileName {
		dumpStatus = "Idle!"
	}
	log.Printf("Dump spent: " + shortDur(workTime(workStart)))
}

func importTable(db *sql.DB, fileName string, tableName string) {
	workStart := time.Now()
	importStatus = "Import in progress!"
	lastFilename = fileName

	log.Printf("Starting import process")

	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	filePath := dir + "/" + fileName
	mysql.RegisterLocalFile(filePath)
	query := "LOAD DATA LOCAL INFILE '" + filePath + "' INTO TABLE " + tableName + " FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES"
	_, err = db.Exec(query)
	if err != nil {
		log.Printf("We hawe some errors: " + err.Error())
	}
	if lastFilename == fileName {
		importStatus = "Idle!"
	}
	log.Printf("Import spent: " + shortDur(workTime(workStart)))
}

func printWelcome() {
	log.Printf("Hello! let's dump!")
}

func getUUID() string {
	newUUID, _ := uuid.NewUUID()
	return newUUID.String()
}

func uptime() time.Duration {
	return time.Since(startTime)
}

func workTime(runTime time.Time) time.Duration {
	return time.Since(runTime)
}

func shortDur(d time.Duration) string {
	d = d.Round(time.Second)
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}
