package main

import (
	"fmt"
	//"time"
	"database/sql"
	_ "github.com/go-sql-driver/mysql" // anonymous import; causes mysql driver to register with database/sql.
)

// setup: go get -u github.com/go-sql-driver/mysql

// reference: http://go-database-sql.org/accessing.html

func main() {

	// Open preps, but does not open a connection to the database. Connections are handled lazily, with
	// a connection pool.
	db, err := sql.Open("mysql", "jaten:Blah0987@tcp(127.0.0.1:3306)/jason") // "jaten:Blah0987@/jason") // user:passwd@/database
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(fmt.Sprintf("could not ping database 'jason': %s", err))
	}

	//fmt.Printf("db = %#v\n", db)

	// Connect and check the server version
	var version string
	db.QueryRow("SELECT VERSION();").Scan(&version)
	fmt.Println("Connected to version:", version)

	//val := read(db)
	//write(db, val + 1)

	fmt.Printf("drop table 'one'\n")
	drop(db)

	fmt.Printf("create table 'one'\n")
	create(db)

	fmt.Printf("deleting all rows in table 'one', just for demonstration purposes.\n")
	del(db)

	fmt.Printf("reading and writing inside a transaction.\n")
	transact(db)

	fmt.Printf("reading and writing inside a transaction again.\n")
	transact(db)

	fmt.Printf("reading everything.\n")
	read(db)
}

func read(db *sql.DB) int {
	id := int64(0)
	a := ""
	b := 0
	//var tm time.Time
	tm := ""

	//rows, err := db.Query("select * from one where id = ?", 1)
	rows, err := db.Query("select * from one")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &a, &b, &tm)
		if err != nil {
			panic(err)
		}
		fmt.Printf("id=%v   a=%v   b=%v   tm=%v\n", id, a, b, tm)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}

	return b
}

func write(db *sql.DB, val int) {
	stmt, err := db.Prepare("insert into one (a,b) values (?, ?)")
	if err != nil {
		panic(err)
	}
	res, err := stmt.Exec("Dolly", val)
	if err != nil {
		panic(err)
	}
	lastId, err := res.LastInsertId()
	if err != nil {
		panic(err)
	}
	rowCnt, err := res.RowsAffected()
	if err != nil {
		panic(err)
	}
	fmt.Printf("wrote to id=%d, with %d row(s) affected\n", lastId, rowCnt)
}

func del(db *sql.DB) {
	_, err := db.Exec("DELETE FROM one")
	if err != nil {
		panic(err)
	}

}

func transact(db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	val := read(db)
	write(db, val+1)

	tx.Commit()
	//tx.Rollback()
}

func create(db *sql.DB) {
	_, err := db.Exec("create table one (id bigint not null auto_increment primary key, a varchar(255), b int, tm timestamp)")
	if err != nil {
		panic(err)
	}
}

func drop(db *sql.DB) {
	db.Exec("drop table one")
	//	_, err := db.Exec("drop table one")
	//	if err != nil {
	//		panic(err)
	//	}
}
