package user

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq" // PostgreSQL driver
	"heckel.io/ntfy/v2/log"
)

// PostgreSQL-specific queries
const (
	pgCreateTablesQueries = `
		BEGIN;
		CREATE TABLE IF NOT EXISTS tier (
			id TEXT PRIMARY KEY,
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			messages_limit INT NOT NULL,
			messages_expiry_duration INT NOT NULL,
			emails_limit INT NOT NULL,
			calls_limit INT NOT NULL,
			reservations_limit INT NOT NULL,
			attachment_file_size_limit INT NOT NULL,
			attachment_total_size_limit INT NOT NULL,
			attachment_expiry_duration INT NOT NULL,
			attachment_bandwidth_limit INT NOT NULL,
			stripe_monthly_price_id TEXT,
			stripe_yearly_price_id TEXT
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_tier_code ON tier (code);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_tier_stripe_monthly_price_id ON tier (stripe_monthly_price_id);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_tier_stripe_yearly_price_id ON tier (stripe_yearly_price_id);
		CREATE TABLE IF NOT EXISTS "user" (
		    id TEXT PRIMARY KEY,
			tier_id TEXT,
			"user" TEXT NOT NULL,
			pass TEXT NOT NULL,
			role TEXT CHECK (role IN ('anonymous', 'admin', 'user')) NOT NULL,
			prefs JSON NOT NULL DEFAULT '{}',
			sync_topic TEXT NOT NULL,
			provisioned INT NOT NULL,
			stats_messages INT NOT NULL DEFAULT 0,
			stats_emails INT NOT NULL DEFAULT 0,
			stats_calls INT NOT NULL DEFAULT 0,
			stripe_customer_id TEXT,
			stripe_subscription_id TEXT,
			stripe_subscription_status TEXT,
			stripe_subscription_interval TEXT,
			stripe_subscription_paid_until INT,
			stripe_subscription_cancel_at INT,
			created INT NOT NULL,
			deleted INT,
		    FOREIGN KEY (tier_id) REFERENCES tier (id)
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_user ON "user" ("user");
		CREATE UNIQUE INDEX IF NOT EXISTS idx_user_stripe_customer_id ON "user" (stripe_customer_id);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_user_stripe_subscription_id ON "user" (stripe_subscription_id);
		CREATE TABLE IF NOT EXISTS user_access (
			user_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			read INT NOT NULL,
			write INT NOT NULL,
			owner_user_id TEXT,
			provisioned INT NOT NULL,
			PRIMARY KEY (user_id, topic),
			FOREIGN KEY (user_id) REFERENCES "user" (id) ON DELETE CASCADE,
		    FOREIGN KEY (owner_user_id) REFERENCES "user" (id) ON DELETE CASCADE
		);
		CREATE TABLE IF NOT EXISTS user_token (
			user_id TEXT NOT NULL,
			token TEXT NOT NULL,
			label TEXT NOT NULL,
			last_access INT NOT NULL,
			last_origin TEXT NOT NULL,
			expires INT NOT NULL,
			provisioned INT NOT NULL,
			PRIMARY KEY (user_id, token),
			FOREIGN KEY (user_id) REFERENCES "user" (id) ON DELETE CASCADE
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_user_token ON user_token (token);
		CREATE TABLE IF NOT EXISTS user_phone (
			user_id TEXT NOT NULL,
			phone_number TEXT NOT NULL,
			PRIMARY KEY (user_id, phone_number),
			FOREIGN KEY (user_id) REFERENCES "user" (id) ON DELETE CASCADE
		);
		CREATE TABLE IF NOT EXISTS schema_version (
			id INT PRIMARY KEY,
			version INT NOT NULL
		);
		INSERT INTO "user" (id, "user", pass, role, sync_topic, provisioned, created)
		VALUES ('` + everyoneID + `', '*', '', 'anonymous', '', 0, EXTRACT(EPOCH FROM NOW())::INT)
		ON CONFLICT (id) DO NOTHING;
		COMMIT;
	`

	pgCurrentSchemaVersion     = 1
	pgInsertSchemaVersion      = `INSERT INTO schema_version VALUES (1, $1)`
	pgUpdateSchemaVersion      = `UPDATE schema_version SET version = $1 WHERE id = 1`
	pgSelectSchemaVersionQuery = `SELECT version FROM schema_version WHERE id = 1`
)

// newPgManager creates a new PostgreSQL-backed user manager
func newPgManager(config *Config) (*Manager, error) {
	connStr := strings.TrimPrefix(config.Filename, "postgres:")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	if err := setupPgDB(db); err != nil {
		return nil, err
	}
	if err := runPgStartupQueries(db, config.StartupQueries); err != nil {
		return nil, err
	}
	return &Manager{
		config:     config,
		db:         db,
		statsQueue: make(map[string]*Stats),
		tokenQueue: make(map[string]*TokenUpdate),
	}, nil
}

func runPgStartupQueries(db *sql.DB, startupQueries string) error {
	if startupQueries != "" {
		if _, err := db.Exec(startupQueries); err != nil {
			return err
		}
	}
	return nil
}

func setupPgDB(db *sql.DB) error {
	// If 'schema_version' table does not exist, this must be a new database
	rowsSV, err := db.Query(pgSelectSchemaVersionQuery)
	if err != nil {
		return setupNewPgDB(db)
	}
	defer rowsSV.Close()

	// If 'schema_version' table exists, read version and potentially upgrade
	schemaVersion := 0
	if !rowsSV.Next() {
		// Table exists but no rows, insert version
		return setupNewPgDB(db)
	}
	if err := rowsSV.Scan(&schemaVersion); err != nil {
		return err
	}
	rowsSV.Close()

	// Do migrations
	if schemaVersion == pgCurrentSchemaVersion {
		return nil
	} else if schemaVersion > pgCurrentSchemaVersion {
		return fmt.Errorf("unexpected schema version: version %d is higher than current version %d", schemaVersion, pgCurrentSchemaVersion)
	}

	// No migrations needed yet for PG (starting at version 1)
	log.Tag(tag).Info("PostgreSQL user database schema is up to date (version %d)", schemaVersion)
	return nil
}

func setupNewPgDB(db *sql.DB) error {
	if _, err := db.Exec(pgCreateTablesQueries); err != nil {
		return err
	}
	if _, err := db.Exec(pgInsertSchemaVersion, pgCurrentSchemaVersion); err != nil {
		return err
	}
	return nil
}

