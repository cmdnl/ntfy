package server

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// PostgreSQL-specific queries
const (
	pgCreateWebPushSubscriptionsTableQuery = `
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
		CREATE TABLE IF NOT EXISTS schema_version (
			id INT PRIMARY KEY,
			version INT NOT NULL
		);			
		COMMIT;
	`

	// Schema management queries
	pgCurrentWebPushSchemaVersion     = 1
	pgInsertWebPushSchemaVersion      = `INSERT INTO schema_version VALUES (1, $1)`
	pgSelectWebPushSchemaVersionQuery = `SELECT version FROM schema_version WHERE id = 1`
)

// PostgreSQL-specific webpush queries
var pgWebPushQueries = &webPushQueries{
	selectSubscriptionIDByEndpoint:        `SELECT id FROM subscription WHERE endpoint = $1`,
	selectSubscriptionCountBySubscriberIP: `SELECT COUNT(*) FROM subscription WHERE subscriber_ip = $1`,
	selectSubscriptionsForTopic: `
		SELECT id, endpoint, key_auth, key_p256dh, user_id
		FROM subscription_topic st
		JOIN subscription s ON s.id = st.subscription_id
		WHERE st.topic = $1
		ORDER BY endpoint
	`,
	selectSubscriptionsExpiringSoon: `
		SELECT id, endpoint, key_auth, key_p256dh, user_id 
		FROM subscription 
		WHERE warned_at = 0 AND updated_at <= $1
	`,
	insertSubscription: `
		INSERT INTO subscription (id, endpoint, key_auth, key_p256dh, user_id, subscriber_ip, updated_at, warned_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (endpoint) 
		DO UPDATE SET key_auth = EXCLUDED.key_auth, key_p256dh = EXCLUDED.key_p256dh, user_id = EXCLUDED.user_id, subscriber_ip = EXCLUDED.subscriber_ip, updated_at = EXCLUDED.updated_at, warned_at = EXCLUDED.warned_at
	`,
	updateSubscriptionWarningSent:     `UPDATE subscription SET warned_at = $1 WHERE id = $2`,
	deleteSubscriptionByEndpoint:      `DELETE FROM subscription WHERE endpoint = $1`,
	deleteSubscriptionByUserID:        `DELETE FROM subscription WHERE user_id = $1`,
	deleteSubscriptionByAge:           `DELETE FROM subscription WHERE updated_at <= $1`,
	insertSubscriptionTopic:           `INSERT INTO subscription_topic (subscription_id, topic) VALUES ($1, $2)`,
	deleteSubscriptionTopicAll:        `DELETE FROM subscription_topic WHERE subscription_id = $1`,
	deleteSubscriptionTopicWithoutSub: `DELETE FROM subscription_topic WHERE subscription_id NOT IN (SELECT id FROM subscription)`,
}

func newPgWebPushStore(connStr, startupQueries string) (*webPushStore, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	if err := setupPgWebPushDB(db); err != nil {
		return nil, err
	}
	if err := runPgWebPushStartupQueries(db, startupQueries); err != nil {
		return nil, err
	}
	return &webPushStore{
		db:      db,
		queries: pgWebPushQueries,
	}, nil
}

func setupPgWebPushDB(db *sql.DB) error {
	// If 'schema_version' table does not exist, this must be a new database
	rows, err := db.Query(pgSelectWebPushSchemaVersionQuery)
	if err != nil {
		return setupNewPgWebPushDB(db)
	}
	defer rows.Close()

	// If table exists but no rows, also create new
	if !rows.Next() {
		return setupNewPgWebPushDB(db)
	}
	return nil
}

func setupNewPgWebPushDB(db *sql.DB) error {
	if _, err := db.Exec(pgCreateWebPushSubscriptionsTableQuery); err != nil {
		return err
	}
	if _, err := db.Exec(pgInsertWebPushSchemaVersion, pgCurrentWebPushSchemaVersion); err != nil {
		return err
	}
	return nil
}

func runPgWebPushStartupQueries(db *sql.DB, startupQueries string) error {
	if startupQueries != "" {
		if _, err := db.Exec(startupQueries); err != nil {
			return err
		}
	}
	return nil
}

