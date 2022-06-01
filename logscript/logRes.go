package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"github.com/gorilla/mux"
	"net/http"
	"time"
	"strconv"
	"log"
)

type JsonResponse struct {
	Type    string           `json:"type"`
	Message string           `json:"message"`
}
var fname string = "log" + strconv.FormatInt(time.Now().Unix(), 10) + ".csv"
type queryLog struct {
	Querier string `json:"querier"`
	Query string `json:"query"`
	Result string `json:"result"`
}


// https://stackoverflow.com/questions/17629451/append-slice-to-csv-golang
func addLog(w http.ResponseWriter, r *http.Request)  {
	// read the file
	//fname string, column []string
	querier := r.FormValue("querier")
	query := r.FormValue("query")
	result := r.FormValue("result")
	// fmt.Println(querier, query, result)
	var response = JsonResponse{}
	if querier == "" || query == "" || result == "" {
		fmt.Println(querier, query, result)
		status := 400
		response := JsonResponse{Type: "Failure", Message: "Entries are missing!"}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(response)
		return
	}
	column := []string{querier, query, result}
	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Error: ", err)
		status := 500
		response := JsonResponse{Type: "Failure", Message: "Something wrong"}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(response)
		return
	}
	writer := csv.NewWriter(f)
	writer.Write(column)
	writer.Flush()
	status := 201
	response = JsonResponse{Type: "success", Message: "The entry has been inserted successfully!"}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}
func main() {
	router := mux.NewRouter()

	router.HandleFunc("/add_log/", addLog).Methods("POST")
	log.Fatal(http.ListenAndServe(":8000", router))
}
