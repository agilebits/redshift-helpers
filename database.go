package rh

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// DatabaseConfig describes database connection parameters
type DatabaseConfig struct {
	Driver   string
	URL      string
	Username string
	Password string
}

// OpenDatabase will create a new database connection. This connection must be closed when it is no longer needed.
func OpenDatabase(config *DatabaseConfig) (*sqlx.DB, error) {
	var url string

	switch config.Driver {
	case "mysql":
		url = fmt.Sprintf("%s:%s@%s", config.Username, config.Password, config.URL)
	case "postgres":
		url = fmt.Sprintf("postgres://%s:%s@%s", config.Username, config.Password, config.URL)
	default:
		return nil, fmt.Errorf("unsupported database driver %+q in OpenDatabase", config.Driver)
	}

	database, err := sqlx.Connect(config.Driver, url)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to OpenDatabase, cannot connect to %+q", config.URL)
	}

	if err := database.Ping(); err != nil {
		return nil, errors.Wrapf(err, "failed to OpenDatabase, ping failed %+q", config.URL)
	}

	return database, nil
}
