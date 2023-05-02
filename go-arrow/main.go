package main

import (
	"io"
	"log"

	"database/sql"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/csv"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/go-sql-driver/mysql"
)

func main() {
	// Create Apache Arrow record
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"str-0", "str-1", "str-2", "str-3", "str-4", "str-5", "str-6", "str-7", "str-8", "str-9"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	// Create connection
	db, err := sql.Open("mysql", "root:1@tcp(127.0.0.1:5506)/db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Prepare table
	_, err = db.Exec("DROP TABLE IF EXISTS t")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("CREATE TABLE t(i64 BIGINT, f64 FLOAT, str TEXT)")
	if err != nil {
		log.Fatal(err)
	}

	// Create Pipe reader and writer
	pr, pw := io.Pipe()

	// Asynchronously write CSV data to the pipe
	go func() {
		defer pw.Close()
		w := csv.NewWriter(pw, schema, csv.WithComma('\t'))
		err := w.Write(rec)
		if err != nil {
			log.Fatal(err)
		}

		err = w.Flush()
		if err != nil {
			log.Fatal(err)
		}

		err = w.Error()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Send LOAD DATA query to the database
	mysql.RegisterReaderHandler("arrow", func() io.Reader {
		return pr
	})
	_, err = db.Exec("LOAD DATA LOCAL INFILE 'Reader::arrow' INTO TABLE t")
	if err != nil {
		log.Fatal(err)
	}
}
