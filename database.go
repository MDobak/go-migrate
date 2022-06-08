package migrate

import (
	"context"
	"database/sql"
	"time"

	"github.com/mdobak/go-xerrors"
)

type sqldb interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type sqldbBeginTx interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type sqldbTX interface {
	Commit() error
	Rollback() error
}

const (
	selectMigrationsSQL = `SELECT * FROM migrations ORDER BY "version" ASC`
	insertMigrationSQL  = `INSERT INTO migrations ("version", "timestamp") VALUES ($1, $2)`
	deleteMigrationSQL  = `DELETE FROM migrations WHERE "version" = $1`
	createTableSQL      = `
	  CREATE TABLE IF NOT EXISTS migrations (
		"version" bigint NOT NULL,
		"timestamp" timestamp NOT NULL,
	  PRIMARY KEY ("version")
	)`
)

type SQLDatabase struct {
	sqldb  sqldb
	inited bool
}

type migration struct {
	Version   int       `db:"version"`
	Timestamp time.Time `db:"timestamp"`
}

func NewSQLDatabase(sqldb sqldb) *SQLDatabase {
	return &SQLDatabase{sqldb: sqldb}
}

func (s *SQLDatabase) List(ctx context.Context) ([]int, error) {
	if err := s.init(ctx); err != nil {
		return nil, err
	}
	var ms []*migration
	rows, err := s.sqldb.QueryContext(ctx, selectMigrationsSQL)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		m := &migration{}
		if err := rows.Scan(&m.Version, &m.Timestamp); err != nil {
			return nil, err
		}
		ms = append(ms, m)
	}
	if err != nil {
		return nil, err
	}
	var vs []int
	for _, m := range ms {
		vs = append(vs, m.Version)
	}
	return vs, nil
}

func (s *SQLDatabase) Migrate(ctx context.Context, actions []Action) error {
	if err := s.init(ctx); err != nil {
		return err
	}
	for _, action := range actions {
		var err error
		switch action.Direction {
		case Up:
			err = s.up(ctx, action)
		case Down:
			err = s.down(ctx, action)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLDatabase) up(ctx context.Context, action Action) error {
	return transaction(ctx, s.sqldb, func(db sqldb) error {
		_, err := db.ExecContext(ctx, action.Migration)
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, insertMigrationSQL, action.Version, time.Now())
		return err
	})
}

func (s *SQLDatabase) down(ctx context.Context, action Action) error {
	return transaction(ctx, s.sqldb, func(db sqldb) error {
		_, err := db.ExecContext(ctx, action.Migration)
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, deleteMigrationSQL, action.Version)
		return err
	})
}

func (s *SQLDatabase) init(ctx context.Context) error {
	if s.inited {
		return nil
	}
	s.inited = true
	_, err := s.sqldb.ExecContext(ctx, createTableSQL)
	return err
}

func transaction(ctx context.Context, sqldb sqldb, fn func(db sqldb) error) (err error) {
	if sqldb, ok := sqldb.(sqldbBeginTx); ok {
		tx, err := sqldb.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() {
			if r := recover(); r != nil {
				err = xerrors.Append(err, xerrors.FromRecover(r))
			}
			if err != nil {
				rerr := tx.Rollback()
				if rerr != nil {
					err = xerrors.Append(err, rerr)
				}
			}
		}()
		err = fn(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	return fn(sqldb)
}
