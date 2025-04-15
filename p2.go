package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http" //# Used proper package
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

const (
	maxIdleConns    = 20
	maxOpenConns    = 5
	connMaxLifetime = 60 * time.Second
	dbTimeout       = 2 * time.Second
	rateLimitPerSec = 5
	rateLimitBurst  = 10
)

var db *sql.DB
var campaignSpends = make(map[int]float64) // Changed to int for campaign ID; create index on this column; Better Readability and handling in code
var mu sync.Mutex

type Campaign struct {
	ID     int     `json:"id"`
	Spend  float64 `json:"spend"`
	Budget float64 `json:"budget"`
}

type Service struct {
	mu      sync.Mutex
	db      *sql.DB
	limiter *rate.Limiter
}

func NewService(db *sql.DB) *Service {
	return &Service{
		db:      db,
		limiter: rate.NewLimiter(rate.Limit(rateLimitPerSec), rateLimitBurst),
	}
}

// Middleware for rate limiting
func (s *Service) rateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !s.limiter.Allow() {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			c.Abort()
			return
		}
		c.Next()
	}
}

func initDB() (*sql.DB, error) {
	var err error
	db, err = sql.Open("postgres", "host=localhost port=5432 user=admin dbname=zocket sslmode=disable")
	if err != nil {
		//panic(err)
		return nil, fmt.Errorf("Failed to open database")
	}

	//# configure connection to optimze resources
	db.SetMaxIdleConns(maxIdleConns)
	db.SetMaxIdleConns(maxOpenConns)
	db.SetConnMaxLifetime(connMaxLifetime)

	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("Failed to ping database")
	}

	fmt.Println("Database connected")
	return db, nil
}

// API to update campaign spend
func (s *Service) updateSpend(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), dbTimeout)
	defer cancel()

	campaignID := c.Param("campaign_id")
	var request struct {
		Spend float64 `json:"spend"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	campaignSpends[campaignID] += request.Spend
	// Inefficient: No transactions, no concurrency safety
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to begin transaction"})
		return
	}
	defer tx.Rollback()

	result, err := db.Exec("UPDATE campaigns SET spend = spend + $1 WHERE id = $2",
		request.Spend, campaignID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database update failed"})

		return
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get rows affected"})
		return
	}
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Campaign not found"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to commit transaction"})
		return
	}

	// Race condition here due to concurrent map writes
	var mu sync.Mutex
	mu.Lock()
	// Update the in-memory map
	campaignSpends[campaignID] += request.Spend

	mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"message": "Spend updated"})
}

// API to get campaign budget status
func (s *Service) getBudgetStatus(c *gin.Context) {

	_, cancel := context.WithTimeout(c.Request.Context(), dbTimeout)
	defer cancel()

	campaignID := c.Param("campaign_id")
	// Validate campaignId
	if err := validateCampaignID(campaignID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Inefficient Query: Should only fetch required fields
	var budget, spend float64
	err := db.QueryRow("SELECT budget, spend FROM campaigns WHERE id = $1", campaignID).Scan(&budget, &spend)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Campaign not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch campaign data"})
		return
	}
	remaining := budget - spend
	status := "Active"
	if remaining <= 0 {
		status = "Overspent"
	}
	c.JSON(http.StatusOK, gin.H{
		"campaign_id": campaignID,
		"budget":      budget,
		"spend":       spend,
		"remaining":   remaining,
		"status":      status,
	})
}

func validateCampaignID(campaignID int) error {
	if campaignID <= 0 {
		return fmt.Errorf("Invalid campaign ID")
	}
	mu.Lock()
	defer mu.Unlock()
	if _, exists := campaignSpends[campaignID]; !exists {
		return fmt.Errorf("Campaign not found")
	}
	return nil
}

func main() {
	var s *Service
	db, err := initDB() //# Proper Handling of DB connection
	if err != nil {
		panic(fmt.Errorf("Database Connection error %v : ", err))
	}
	defer db.Close()

	r := gin.Default()

	// Middleware to handle CORS
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})
	// Middleware to handle logging
	r.Use(func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start)
		fmt.Printf("Request: %s %s took %v\n", c.Request.Method, c.Request.URL, duration)
	})

	// Middleware to handle authentication
	r.Use(func(c *gin.Context) {
		token := c.Request.Header.Get("Authorization")
		if token == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Authorization token required"})
			c.Abort()
			return
		}
	})

	campaigns := r.Group("/campaigns")
	{
		campaigns.POST("/:campaign_id/spend", s.updateSpend)
		campaigns.GET("/:campaign_id/budget-status", s.getBudgetStatus)
	}

	err = r.Run(":8080") //# error handling
	if err != nil {
		fmt.Errorf("Starting server failed : %v", err)
	}
}
