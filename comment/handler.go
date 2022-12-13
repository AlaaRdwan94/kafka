package comment

import (
	"encoding/json"
	"fmt"
	"net/http"
	"kafka/connection"
)

func CommentHandler() {
	http.HandleFunc("/", CreateComment)
	http.HandleFunc("/get", GetComment)
	http.ListenAndServe(":6666",nil)
}

func CreateComment(w http.ResponseWriter, r *http.Request) {
	var comment Comment
	err := json.NewDecoder(r.Body).Decode(&comment)
	if err != nil {
		fmt.Printf("error in request : %v", err)
	}
	// convert to bytes and send it to kafka comments tobic
	cmntbyte , err := json.Marshal(&comment)
	if err != nil {
		fmt.Printf("error converting to bytes : %v", err)
	}
	err = connection.PushCommentToQueue("comments",cmntbyte)
	if err != nil {
		fmt.Println("error while pushing : ", err)
	}
	// write a response 
	fmt.Fprintf(w," accept comment %v from the request ", comment.Text)
	//w.Write([]byte("this is also a reply"))
}

type Comment struct {
    Text string `form:"text" json:"text"`
}

func GetComment(w http.ResponseWriter, r *http.Request) {
	connection.CommentConsumer()
	// write a response 
	fmt.Fprintf(w," accept commentfrom the request ")
	//w.Write([]byte("this is also a reply"))
}
