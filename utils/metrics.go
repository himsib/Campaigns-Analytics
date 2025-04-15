package utils

import "campaign-analytics/models"

func ComputeMetrics(data models.CampaignData) map[string]float64 {
	ctr := float64(data.Clicks) / float64(data.Impressions)
	cpa := data.Cost / float64(data.Conversions)
	roas := data.Revenue / data.Cost

	return map[string]float64{
		"CTR":   ctr,
		"CPA":   cpa,
		"ROAS":  roas,
		"Spend": data.Cost,
	}
}

func ValidateToken(token string) bool {
	return token == "valid_token"
}
