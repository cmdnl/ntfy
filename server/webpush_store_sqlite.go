package server

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// SQLite-specific queries
const (
	sqliteCreateWebPushSubscriptionsTableQuery = `
		BEGIN;
		CREATE TABLE IF NOT EXISTS subscription (
			id TEXT PRIMARY KEY,
			endpoint TEXT NOT NULL,
			key_auth TEXT NOT NULL,
			key_p256dh TEXT NOT NULL,
			user_id TEXT NOT NULL,		
			subscriber_ip TEXT NOT NULL,
			updated_at INT NOT NULL,
			warned_at INT NOT NULL DEFAULT 0
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_endpoint ON subscription (endpoint);
		CREATE INDEX IF NOT EXISTS idx_subscriber_ip ON subscription (subscriber_ip);
		CREATE TABLE IF NOT EXISTS subscription_topic (
			subscription_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			PRIMARY KEY (subscription_id, topic),
			FOREIGN KEY (subscription_id) REFERENCES subscription (id) ON DELETE CASCADE
		);
		CREATE INDEX IF NOT EXISTS idx_topic ON subscription_topic (topic);
		CREATE TABLE IF NOT EXISTS schemaVersion (
			id INT PRIMARY KEY,
			version INT NOT NULL
		);			
		COMMIT;
	`
	sqliteWebPushBuiltinStartupQueries = `
		PRAGMA foreign_keys = ON;
	`

	// Schema management queries
	sqliteCurrentWebPushSchemaVersion     = 1
	sqliteInsertWebPushSchemaVersion      = `INSERT INTO schemaVersion VALUES (1, ?)`
	sqliteSelectWebPushSchemaVersionQuery = `SELECT version FROM schemaVersion WHERE id = 1`
)

// SQLite-specific webpush queries
var sqliteWebPushQueries = &webPushQueries{
	selectSubscriptionIDByEndpoint:        `SELECT id FROM subscription WHERE endpoint = ?`,
	selectSubscriptionCountBySubscriberIP: `SELECT COUNT(*) FROM subscription WHERE subscriber_ip = ?`,
	selectSubscriptionsForTopic: `
		SELECT id, endpoint, key_auth, key_p256dh, user_id
		FROM subscription_topic st
		JOIN subscription s ON s.id = st.subscription_id
		WHERE st.topic = ?
		ORDER BY endpoint
	`,
	selectSubscriptionsExpiringSoon: `
		SELECT id, endpoint, key_auth, key_p256dh, user_id 
		FROM subscription 
		WHERE warned_at = 0 AND updated_at <= ?
	`,
	insertSubscription: `
		INSERT INTO subscription (id, endpoint, key_auth, key_p256dh, user_id, subscriber_ip, updated_at, warned_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (endpoint) 
		DO UPDATE SET key_auth = excluded.key_auth, key_p256dh = excluded.key_p256dh, user_id = excluded.user_id, subscriber_ip = excluded.subscriber_ip, updated_at = excluded.updated_at, warned_at = excluded.warned_at
	`,
	updateSubscriptionWarningSent:       `UPDATE subscription SET warned_at = ? WHERE id = ?`,
	deleteSubscriptionByEndpoint:        `DELETE FROM subscription WHERE endpoint = ?`,
	deleteSubscriptionByUserID:          `DELETE FROM subscription WHERE user_id = ?`,
	deleteSubscriptionByAge:             `DELETE FROM subscription WHERE updated_at <= ?`,
	insertSubscriptionTopic:             `INSERT INTO subscription_topic (subscription_id, topic) VALUES (?, ?)`,
	deleteSubscriptionTopicAll:          `DELETE FROM subscription_topic WHERE subscription_id = ?`,
	deleteSubscriptionTopicWithoutSub:   `DELETE FROM subscription_topic WHERE subscription_id NOT IN (SELECT id FROM subscription)`,
}

func newSqliteWebPushStore(filename, startupQueries string) (*webPushStore, error) {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}
	if err := setupSqliteWebPushDB(db); err != nil {
		return nil, err
	}
	if err := runSqliteWebPushStartupQueries(db, startupQueries); err != nil {
		return nil, err
	}
	return &webPushStore{
		db:      db,
		queries: sqliteWebPushQueries,
	}, nil
}

func setupSqliteWebPushDB(db *sql.DB) error {
	// If 'schemaVersion' table does not exist, this must be a new database
	rows, err := db.Query(sqliteSelectWebPushSchemaVersionQuery)
	if err != nil {
		return setupNewSqliteWebPushDB(db)
	}
	return rows.Close()
}

func setupNewSqliteWebPushDB(db *sql.DB) error {
	if _, err := db.Exec(sqliteCreateWebPushSubscriptionsTableQuery); err != nil {
		return err
	}
	if _, err := db.Exec(sqliteInsertWebPushSchemaVersion, sqliteCurrentWebPushSchemaVersion); err != nil {
		return err
	}
	return nil
}

func runSqliteWebPushStartupQueries(db *sql.DB, startupQueries string) error {
	if startupQueries != "" {
		if _, err := db.Exec(startupQueries); err != nil {
			return err
		}
	}
	if _, err := db.Exec(sqliteWebPushBuiltinStartupQueries); err != nil {
		return err
	}
	return nil
}

