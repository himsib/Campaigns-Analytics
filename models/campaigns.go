package models

import "github.com/goravel/framework/database/db"

type CampaignData struct {
	Impressions int
	Clicks      int
	Conversions int
	Cost        float64
	Revenue     float64
}

func GetCampaignData(campaignID, platform, startDate, endDate string) (CampaignData, error) {

	db := db.New()
	// Example query to fetch campaign data
	//modify this query to fetch data in chunks using offset and append for single file

	query := "SELECT impressions, clicks, conversions, cost, revenue FROM campaigns WHERE id = ? AND platform = ? AND date BETWEEN ? AND ?"
	rows, err := db.Query(query, campaignID, platform, startDate, endDate)
	if err != nil {
		return CampaignData{}, err
	}
	defer rows.Close()
	var impressions, clicks, conversions int
	var cost, revenue float64
	for rows.Next() {
		err := rows.Scan(&impressions, &clicks, &conversions, &cost, &revenue)
		if err != nil {
			return CampaignData{}, err
		}
	}

	return CampaignData{
		Impressions: 1000,
		Clicks:      100,
		Conversions: 10,
		Cost:        500,
		Revenue:     1500,
	}, nil
}
