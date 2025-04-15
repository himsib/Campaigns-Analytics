package services

import (
	"campaign-analytics/factory"
	"campaign-analytics/utils"
	"errors"
)

func FetchInsights(campaignID, platform, startDate, endDate string) (map[string]float64, error) {
	dataFetcher, err := factory.GetCampaignDataFetcher(platform)
	if err != nil {
		return nil, err
	}

	data, err := dataFetcher.FetchData(campaignID, startDate, endDate)
	if err != nil {
		return nil, err
	}

	if data.Impressions == 0 {
		return nil, errors.New("no impressions data available")
	}

	metrics := utils.ComputeMetrics(data)
	return metrics, nil
}
