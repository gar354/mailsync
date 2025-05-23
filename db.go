package main

import (
	"context"
	"github.com/jackc/pgx/v5"
)

type DBState struct {
	Ctx  *context.Context
	Conn *pgx.Conn
}

func DBConnect(connUrl string) (DBState, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connUrl)
	if err != nil {
		return DBState{}, err
	}
	return DBState{
		Ctx:  &ctx,
		Conn: conn,
	}, nil
}

func (db DBState) QueryGrades(grades []int32) (pgx.Rows, error) {
	rows, err := db.Conn.Query(*db.Ctx,
		`SELECT email, first_name, last_name FROM parents WHERE grade && $1`, grades)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (db DBState) DBClose() error {
	return db.Conn.Close(*db.Ctx)
}
