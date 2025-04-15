package fetcher

type CampaignData struct {
	Impressions int
	Clicks      int
	Conversions int
	Cost        float64
	Revenue     float64
}

type MetaFetcher struct{}

func (m *MetaFetcher) FetchData(campaignID, startDate, endDate string) (CampaignData, error) {
	// Custom Api logic to fetch data from Meta
	return CampaignData{
		Impressions: 1000,
		Clicks:      100,
		Conversions: 10,
		Cost:        500,
		Revenue:     1500,
	}, nil
}
