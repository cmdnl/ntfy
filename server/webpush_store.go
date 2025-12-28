package server

import (
	"database/sql"
	"errors"
	"net/netip"
	"strings"
	"time"

	"heckel.io/ntfy/v2/util"
)

const (
	subscriptionIDPrefix                     = "wps_"
	subscriptionIDLength                     = 10
	subscriptionEndpointLimitPerSubscriberIP = 10
)

var (
	errWebPushNoRows               = errors.New("no rows found")
	errWebPushTooManySubscriptions = errors.New("too many subscriptions")
	errWebPushUserIDCannotBeEmpty  = errors.New("user ID cannot be empty")
)

// WebPushStore is an interface for storing web push subscriptions
type WebPushStore interface {
	UpsertSubscription(endpoint string, auth, p256dh, userID string, subscriberIP netip.Addr, topics []string) error
	SubscriptionsForTopic(topic string) ([]*webPushSubscription, error)
	SubscriptionsExpiring(warnAfter time.Duration) ([]*webPushSubscription, error)
	MarkExpiryWarningSent(subscriptions []*webPushSubscription) error
	RemoveSubscriptionsByEndpoint(endpoint string) error
	RemoveSubscriptionsByUserID(userID string) error
	RemoveExpiredSubscriptions(expireAfter time.Duration) error
	DB() *sql.DB
	Close() error
}

// webPushQueries holds all the SQL queries used by webPushStore
type webPushQueries struct {
	selectSubscriptionIDByEndpoint        string
	selectSubscriptionCountBySubscriberIP string
	selectSubscriptionsForTopic           string
	selectSubscriptionsExpiringSoon       string
	insertSubscription                    string
	updateSubscriptionWarningSent         string
	deleteSubscriptionByEndpoint          string
	deleteSubscriptionByUserID            string
	deleteSubscriptionByAge               string
	insertSubscriptionTopic               string
	deleteSubscriptionTopicAll            string
	deleteSubscriptionTopicWithoutSub     string
}

type webPushStore struct {
	db      *sql.DB
	queries *webPushQueries
}

// newWebPushStore creates a new webPushStore based on the connection string
func newWebPushStore(filename, startupQueries string) (WebPushStore, error) {
	if strings.HasPrefix(filename, "postgres:") {
		return newPgWebPushStore(strings.TrimPrefix(filename, "postgres:"), startupQueries)
	}
	return newSqliteWebPushStore(filename, startupQueries)
}

// UpsertSubscription adds or updates Web Push subscriptions for the given topics and user ID
func (c *webPushStore) UpsertSubscription(endpoint string, auth, p256dh, userID string, subscriberIP netip.Addr, topics []string) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Read number of subscriptions for subscriber IP address
	rowsCount, err := tx.Query(c.queries.selectSubscriptionCountBySubscriberIP, subscriberIP.String())
	if err != nil {
		return err
	}
	defer rowsCount.Close()
	var subscriptionCount int
	if !rowsCount.Next() {
		return errWebPushNoRows
	}
	if err := rowsCount.Scan(&subscriptionCount); err != nil {
		return err
	}
	if err := rowsCount.Close(); err != nil {
		return err
	}
	// Read existing subscription ID for endpoint (or create new ID)
	rows, err := tx.Query(c.queries.selectSubscriptionIDByEndpoint, endpoint)
	if err != nil {
		return err
	}
	defer rows.Close()
	var subscriptionID string
	if rows.Next() {
		if err := rows.Scan(&subscriptionID); err != nil {
			return err
		}
	} else {
		if subscriptionCount >= subscriptionEndpointLimitPerSubscriberIP {
			return errWebPushTooManySubscriptions
		}
		subscriptionID = util.RandomStringPrefix(subscriptionIDPrefix, subscriptionIDLength)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	// Insert or update subscription
	updatedAt, warnedAt := time.Now().Unix(), 0
	if _, err = tx.Exec(c.queries.insertSubscription, subscriptionID, endpoint, auth, p256dh, userID, subscriberIP.String(), updatedAt, warnedAt); err != nil {
		return err
	}
	// Replace all subscription topics
	if _, err := tx.Exec(c.queries.deleteSubscriptionTopicAll, subscriptionID); err != nil {
		return err
	}
	for _, topic := range topics {
		if _, err = tx.Exec(c.queries.insertSubscriptionTopic, subscriptionID, topic); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SubscriptionsForTopic returns all subscriptions for the given topic
func (c *webPushStore) SubscriptionsForTopic(topic string) ([]*webPushSubscription, error) {
	rows, err := c.db.Query(c.queries.selectSubscriptionsForTopic, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return c.subscriptionsFromRows(rows)
}

// SubscriptionsExpiring returns all subscriptions that have not been updated for a given time period
func (c *webPushStore) SubscriptionsExpiring(warnAfter time.Duration) ([]*webPushSubscription, error) {
	rows, err := c.db.Query(c.queries.selectSubscriptionsExpiringSoon, time.Now().Add(-warnAfter).Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return c.subscriptionsFromRows(rows)
}

// MarkExpiryWarningSent marks the given subscriptions as having received a warning about expiring soon
func (c *webPushStore) MarkExpiryWarningSent(subscriptions []*webPushSubscription) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, subscription := range subscriptions {
		if _, err := tx.Exec(c.queries.updateSubscriptionWarningSent, time.Now().Unix(), subscription.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (c *webPushStore) subscriptionsFromRows(rows *sql.Rows) ([]*webPushSubscription, error) {
	subscriptions := make([]*webPushSubscription, 0)
	for rows.Next() {
		var id, endpoint, auth, p256dh, userID string
		if err := rows.Scan(&id, &endpoint, &auth, &p256dh, &userID); err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, &webPushSubscription{
			ID:       id,
			Endpoint: endpoint,
			Auth:     auth,
			P256dh:   p256dh,
			UserID:   userID,
		})
	}
	return subscriptions, nil
}

// RemoveSubscriptionsByEndpoint removes the subscription for the given endpoint
func (c *webPushStore) RemoveSubscriptionsByEndpoint(endpoint string) error {
	_, err := c.db.Exec(c.queries.deleteSubscriptionByEndpoint, endpoint)
	return err
}

// RemoveSubscriptionsByUserID removes all subscriptions for the given user ID
func (c *webPushStore) RemoveSubscriptionsByUserID(userID string) error {
	if userID == "" {
		return errWebPushUserIDCannotBeEmpty
	}
	_, err := c.db.Exec(c.queries.deleteSubscriptionByUserID, userID)
	return err
}

// RemoveExpiredSubscriptions removes all subscriptions that have not been updated for a given time period
func (c *webPushStore) RemoveExpiredSubscriptions(expireAfter time.Duration) error {
	_, err := c.db.Exec(c.queries.deleteSubscriptionByAge, time.Now().Add(-expireAfter).Unix())
	if err != nil {
		return err
	}
	_, err = c.db.Exec(c.queries.deleteSubscriptionTopicWithoutSub)
	return err
}

// DB returns the underlying database connection (for testing)
func (c *webPushStore) DB() *sql.DB {
	return c.db
}

// Close closes the underlying database connection
func (c *webPushStore) Close() error {
	return c.db.Close()
}
