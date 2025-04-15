//ADS platforms  => Data Ingestion stream  =>  Stream processing  =>  Data storage(ClickHouse/S3)  =>  API Layer  =>

package main

import (
	"campaign-analytics/handlers"
	"campaign-analytics/middleware"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()

	// Apply authentication middleware
	router.Use(middleware.AuthMiddleware())

	// Define routes
	router.Group("/campaign")
	{
		router.GET("/:id/insights", handlers.GetCampaignInsights)
	}
	// Start the server
	router.Run(":8080")
}
