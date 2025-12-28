package user

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/mattn/go-sqlite3"
	"heckel.io/ntfy/v2/log"
	"heckel.io/ntfy/v2/util"
)

var (
	errNoRows = errors.New("no rows found")
)

// SQLite-specific queries
const (
	sqliteCreateTablesQueries = `
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
		CREATE UNIQUE INDEX idx_tier_code ON tier (code);
		CREATE UNIQUE INDEX idx_tier_stripe_monthly_price_id ON tier (stripe_monthly_price_id);
		CREATE UNIQUE INDEX idx_tier_stripe_yearly_price_id ON tier (stripe_yearly_price_id);
		CREATE TABLE IF NOT EXISTS user (
		    id TEXT PRIMARY KEY,
			tier_id TEXT,
			user TEXT NOT NULL,
			pass TEXT NOT NULL,
			role TEXT CHECK (role IN ('anonymous', 'admin', 'user')) NOT NULL,
			prefs JSON NOT NULL DEFAULT '{}',
			sync_topic TEXT NOT NULL,
			provisioned INT NOT NULL,
			stats_messages INT NOT NULL DEFAULT (0),
			stats_emails INT NOT NULL DEFAULT (0),
			stats_calls INT NOT NULL DEFAULT (0),
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
		CREATE UNIQUE INDEX idx_user ON user (user);
		CREATE UNIQUE INDEX idx_user_stripe_customer_id ON user (stripe_customer_id);
		CREATE UNIQUE INDEX idx_user_stripe_subscription_id ON user (stripe_subscription_id);
		CREATE TABLE IF NOT EXISTS user_access (
			user_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			read INT NOT NULL,
			write INT NOT NULL,
			owner_user_id INT,
			provisioned INT NOT NULL,
			PRIMARY KEY (user_id, topic),
			FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE,
		    FOREIGN KEY (owner_user_id) REFERENCES user (id) ON DELETE CASCADE
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
			FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE
		);
		CREATE UNIQUE INDEX idx_user_token ON user_token (token);
		CREATE TABLE IF NOT EXISTS user_phone (
			user_id TEXT NOT NULL,
			phone_number TEXT NOT NULL,
			PRIMARY KEY (user_id, phone_number),
			FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE
		);
		CREATE TABLE IF NOT EXISTS schemaVersion (
			id INT PRIMARY KEY,
			version INT NOT NULL
		);
		INSERT INTO user (id, user, pass, role, sync_topic, provisioned, created)
		VALUES ('` + everyoneID + `', '*', '', 'anonymous', '', false, UNIXEPOCH())
		ON CONFLICT (id) DO NOTHING;
		COMMIT;
	`

	sqliteBuiltinStartupQueries = `
		PRAGMA foreign_keys = ON;
	`
)

// Schema management queries
const (
	sqliteCurrentSchemaVersion     = 6
	sqliteInsertSchemaVersion      = `INSERT INTO schemaVersion VALUES (1, ?)`
	sqliteUpdateSchemaVersion      = `UPDATE schemaVersion SET version = ? WHERE id = 1`
	sqliteSelectSchemaVersionQuery = `SELECT version FROM schemaVersion WHERE id = 1`

	// Migration queries (1->2 through 5->6)
	// These are SQLite-specific due to PRAGMA, ALTER TABLE syntax, etc.
	migrate1To2CreateTablesQueries = `
		ALTER TABLE user RENAME TO user_old;
		CREATE TABLE IF NOT EXISTS tier (
			id TEXT PRIMARY KEY,
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			messages_limit INT NOT NULL,
			messages_expiry_duration INT NOT NULL,
			emails_limit INT NOT NULL,
			reservations_limit INT NOT NULL,
			attachment_file_size_limit INT NOT NULL,
			attachment_total_size_limit INT NOT NULL,
			attachment_expiry_duration INT NOT NULL,
			attachment_bandwidth_limit INT NOT NULL,
			stripe_price_id TEXT
		);
		CREATE UNIQUE INDEX idx_tier_code ON tier (code);
		CREATE UNIQUE INDEX idx_tier_price_id ON tier (stripe_price_id);
		CREATE TABLE IF NOT EXISTS user (
		    id TEXT PRIMARY KEY,
			tier_id TEXT,
			user TEXT NOT NULL,
			pass TEXT NOT NULL,
			role TEXT CHECK (role IN ('anonymous', 'admin', 'user')) NOT NULL,
			prefs JSON NOT NULL DEFAULT '{}',
			sync_topic TEXT NOT NULL,
			stats_messages INT NOT NULL DEFAULT (0),
			stats_emails INT NOT NULL DEFAULT (0),
			stripe_customer_id TEXT,
			stripe_subscription_id TEXT,
			stripe_subscription_status TEXT,
			stripe_subscription_paid_until INT,
			stripe_subscription_cancel_at INT,
			created INT NOT NULL,
			deleted INT,
		    FOREIGN KEY (tier_id) REFERENCES tier (id)
		);
		CREATE UNIQUE INDEX idx_user ON user (user);
		CREATE UNIQUE INDEX idx_user_stripe_customer_id ON user (stripe_customer_id);
		CREATE UNIQUE INDEX idx_user_stripe_subscription_id ON user (stripe_subscription_id);
		CREATE TABLE IF NOT EXISTS user_access (
			user_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			read INT NOT NULL,
			write INT NOT NULL,
			owner_user_id INT,
			PRIMARY KEY (user_id, topic),
			FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE,
		    FOREIGN KEY (owner_user_id) REFERENCES user (id) ON DELETE CASCADE
		);
		CREATE TABLE IF NOT EXISTS user_token (
			user_id TEXT NOT NULL,
			token TEXT NOT NULL,
			label TEXT NOT NULL,
			last_access INT NOT NULL,
			last_origin TEXT NOT NULL,
			expires INT NOT NULL,
			PRIMARY KEY (user_id, token),
			FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE
		);
		CREATE TABLE IF NOT EXISTS schemaVersion (
			id INT PRIMARY KEY,
			version INT NOT NULL
		);
		INSERT INTO user (id, user, pass, role, sync_topic, created)
		VALUES ('u_everyone', '*', '', 'anonymous', '', UNIXEPOCH())
		ON CONFLICT (id) DO NOTHING;
	`
	migrate1To2SelectAllOldUsernamesNoTx = `SELECT user FROM user_old`
	migrate1To2InsertUserNoTx            = `
		INSERT INTO user (id, user, pass, role, sync_topic, created)
		SELECT ?, user, pass, role, ?, UNIXEPOCH() FROM user_old WHERE user = ?
	`
	migrate1To2InsertFromOldTablesAndDropNoTx = `
		INSERT INTO user_access (user_id, topic, read, write)
		SELECT u.id, a.topic, a.read, a.write
		FROM user u
	 	JOIN access a ON u.user = a.user;

		DROP TABLE access;
		DROP TABLE user_old;
	`

	migrate2To3UpdateQueries = `
		ALTER TABLE user ADD COLUMN stripe_subscription_interval TEXT;
		ALTER TABLE tier RENAME COLUMN stripe_price_id TO stripe_monthly_price_id;
		ALTER TABLE tier ADD COLUMN stripe_yearly_price_id TEXT;
		DROP INDEX IF EXISTS idx_tier_price_id;
		CREATE UNIQUE INDEX idx_tier_stripe_monthly_price_id ON tier (stripe_monthly_price_id);
		CREATE UNIQUE INDEX idx_tier_stripe_yearly_price_id ON tier (stripe_yearly_price_id);
	`

	migrate3To4UpdateQueries = `
		ALTER TABLE tier ADD COLUMN calls_limit INT NOT NULL DEFAULT (0);
		ALTER TABLE user ADD COLUMN stats_calls INT NOT NULL DEFAULT (0);
		CREATE TABLE IF NOT EXISTS user_phone (
			user_id TEXT NOT NULL,
			phone_number TEXT NOT NULL,
			PRIMARY KEY (user_id, phone_number),
			FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE
		);
	`

	migrate4To5UpdateQueries = `
		UPDATE user_access SET topic = REPLACE(topic, '_', '\_');
	`

	migrate5To6UpdateQueries = `
		PRAGMA foreign_keys=off;

		-- Alter user table: Add provisioned column
		ALTER TABLE user RENAME TO user_old;
		CREATE TABLE IF NOT EXISTS user (
		    id TEXT PRIMARY KEY,
			tier_id TEXT,
			user TEXT NOT NULL,
			pass TEXT NOT NULL,
			role TEXT CHECK (role IN ('anonymous', 'admin', 'user')) NOT NULL,
			prefs JSON NOT NULL DEFAULT '{}',
			sync_topic TEXT NOT NULL,
			provisioned INT NOT NULL,
			stats_messages INT NOT NULL DEFAULT (0),
			stats_emails INT NOT NULL DEFAULT (0),
			stats_calls INT NOT NULL DEFAULT (0),
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
		INSERT INTO user
		SELECT
		    id,
		    tier_id,
		    user,
		    pass,
		    role,
		    prefs,
		    sync_topic,
		    0, -- provisioned
		    stats_messages,
		    stats_emails,
		    stats_calls,
		    stripe_customer_id,
		    stripe_subscription_id,
		    stripe_subscription_status,
		    stripe_subscription_interval,
		    stripe_subscription_paid_until,
		    stripe_subscription_cancel_at,
		    created,
		    deleted
		FROM user_old;
		DROP TABLE user_old;

		-- Alter user_access table: Add provisioned column
		ALTER TABLE user_access RENAME TO user_access_old;
		CREATE TABLE user_access (
			user_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			read INT NOT NULL,
			write INT NOT NULL,
			owner_user_id INT,
			provisioned INTEGER NOT NULL,
			PRIMARY KEY (user_id, topic),
			FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE,
			FOREIGN KEY (owner_user_id) REFERENCES user (id) ON DELETE CASCADE
		);
		INSERT INTO user_access SELECT *, 0 FROM user_access_old;
		DROP TABLE user_access_old;

		-- Alter user_token table: Add provisioned column
		ALTER TABLE user_token RENAME TO user_token_old;
		CREATE TABLE IF NOT EXISTS user_token (
			user_id TEXT NOT NULL,
			token TEXT NOT NULL,
			label TEXT NOT NULL,
			last_access INT NOT NULL,
			last_origin TEXT NOT NULL,
			expires INT NOT NULL,
			provisioned INT NOT NULL,
			PRIMARY KEY (user_id, token),
			FOREIGN KEY (user_id) REFERENCES user (id) ON DELETE CASCADE
		);
		INSERT INTO user_token SELECT *, 0 FROM user_token_old;
		DROP TABLE user_token_old;

		-- Recreate indices
		CREATE UNIQUE INDEX idx_user ON user (user);
		CREATE UNIQUE INDEX idx_user_stripe_customer_id ON user (stripe_customer_id);
		CREATE UNIQUE INDEX idx_user_stripe_subscription_id ON user (stripe_subscription_id);
		CREATE UNIQUE INDEX idx_user_token ON user_token (token);

		-- Re-enable foreign keys
		PRAGMA foreign_keys=on;
	`
)

var (
	sqliteMigrations = map[int]func(db *sql.DB) error{
		1: migrateFrom1,
		2: migrateFrom2,
		3: migrateFrom3,
		4: migrateFrom4,
		5: migrateFrom5,
	}
)

// newSqliteManager creates a new SQLite-backed Manager
func newSqliteManager(config *Config) (*Manager, error) {
	// Check the parent directory of the database file (makes for friendly error messages)
	parentDir := filepath.Dir(config.Filename)
	if !util.FileExists(parentDir) {
		return nil, fmt.Errorf("user database directory %s does not exist or is not accessible", parentDir)
	}
	// Open DB and run setup queries
	db, err := sql.Open("sqlite3", config.Filename)
	if err != nil {
		return nil, err
	}
	if err := setupSqliteDB(db); err != nil {
		return nil, err
	}
	if err := runSqliteStartupQueries(db, config.StartupQueries); err != nil {
		return nil, err
	}
	return &Manager{
		db:         db,
		config:     config,
		statsQueue: make(map[string]*Stats),
		tokenQueue: make(map[string]*TokenUpdate),
	}, nil
}

func runSqliteStartupQueries(db *sql.DB, startupQueries string) error {
	if startupQueries != "" {
		if _, err := db.Exec(startupQueries); err != nil {
			return err
		}
	}
	if _, err := db.Exec(sqliteBuiltinStartupQueries); err != nil {
		return err
	}
	return nil
}

func setupSqliteDB(db *sql.DB) error {
	// If 'schemaVersion' table does not exist, this must be a new database
	rowsSV, err := db.Query(sqliteSelectSchemaVersionQuery)
	if err != nil {
		return setupNewSqliteDB(db)
	}
	defer rowsSV.Close()

	// If 'schemaVersion' table exists, read version and potentially upgrade
	schemaVersion := 0
	if !rowsSV.Next() {
		return errors.New("cannot determine schema version: database file may be corrupt")
	}
	if err := rowsSV.Scan(&schemaVersion); err != nil {
		return err
	}
	rowsSV.Close()

	// Do migrations
	if schemaVersion == sqliteCurrentSchemaVersion {
		return nil
	} else if schemaVersion > sqliteCurrentSchemaVersion {
		return fmt.Errorf("unexpected schema version: version %d is higher than current version %d", schemaVersion, sqliteCurrentSchemaVersion)
	}
	for i := schemaVersion; i < sqliteCurrentSchemaVersion; i++ {
		fn, ok := sqliteMigrations[i]
		if !ok {
			return fmt.Errorf("cannot find migration step from schema version %d to %d", i, i+1)
		} else if err := fn(db); err != nil {
			return err
		}
	}
	return nil
}

func setupNewSqliteDB(db *sql.DB) error {
	if _, err := db.Exec(sqliteCreateTablesQueries); err != nil {
		return err
	}
	if _, err := db.Exec(sqliteInsertSchemaVersion, sqliteCurrentSchemaVersion); err != nil {
		return err
	}
	return nil
}

func migrateFrom1(db *sql.DB) error {
	log.Tag(tag).Info("Migrating user database schema: from 1 to 2")
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Rename user -> user_old, and create new tables
	if _, err := tx.Exec(migrate1To2CreateTablesQueries); err != nil {
		return err
	}
	// Insert users from user_old into new user table, with ID and sync_topic
	rows, err := tx.Query(migrate1To2SelectAllOldUsernamesNoTx)
	if err != nil {
		return err
	}
	defer rows.Close()
	usernames := make([]string, 0)
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return err
		}
		usernames = append(usernames, username)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, username := range usernames {
		userID := util.RandomStringPrefix(userIDPrefix, userIDLength)
		syncTopic := util.RandomStringPrefix(syncTopicPrefix, syncTopicLength)
		if _, err := tx.Exec(migrate1To2InsertUserNoTx, userID, syncTopic, username); err != nil {
			return err
		}
	}
	// Migrate old "access" table to "user_access" and drop "access" and "user_old"
	if _, err := tx.Exec(migrate1To2InsertFromOldTablesAndDropNoTx); err != nil {
		return err
	}
	if _, err := tx.Exec(sqliteUpdateSchemaVersion, 2); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func migrateFrom2(db *sql.DB) error {
	log.Tag(tag).Info("Migrating user database schema: from 2 to 3")
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(migrate2To3UpdateQueries); err != nil {
		return err
	}
	if _, err := tx.Exec(sqliteUpdateSchemaVersion, 3); err != nil {
		return err
	}
	return tx.Commit()
}

func migrateFrom3(db *sql.DB) error {
	log.Tag(tag).Info("Migrating user database schema: from 3 to 4")
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(migrate3To4UpdateQueries); err != nil {
		return err
	}
	if _, err := tx.Exec(sqliteUpdateSchemaVersion, 4); err != nil {
		return err
	}
	return tx.Commit()
}

func migrateFrom4(db *sql.DB) error {
	log.Tag(tag).Info("Migrating user database schema: from 4 to 5")
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(migrate4To5UpdateQueries); err != nil {
		return err
	}
	if _, err := tx.Exec(sqliteUpdateSchemaVersion, 5); err != nil {
		return err
	}
	return tx.Commit()
}

func migrateFrom5(db *sql.DB) error {
	log.Tag(tag).Info("Migrating user database schema: from 5 to 6")
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(migrate5To6UpdateQueries); err != nil {
		return err
	}
	if _, err := tx.Exec(sqliteUpdateSchemaVersion, 6); err != nil {
		return err
	}
	return tx.Commit()
}

// isSqliteConstraintUniqueError checks if the error is a SQLite unique constraint error
func isSqliteConstraintUniqueError(err error) bool {
	if sqliteErr, ok := err.(sqlite3.Error); ok && sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique {
		return true
	}
	return errors.Is(err, sqlite3.ErrConstraintUnique)
}

