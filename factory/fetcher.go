package factory

import (
	"errors"
)

// MetaFetcher is a struct that implements the CampaignDataFetcher interface.
type MetaFetcher struct{}
type GoogleFetcher struct{}
type LinkedInFetcher struct{}
type TikTokFetcher struct{}

type CampaignDataFetcher interface {
	FetchData() (interface{}, error)
}
type CampaignData struct {
	Impressions int
	Clicks      int
	Conversions int
	Cost        float64
	Revenue     float64
}

/* Implementing Polymorphism */

// FetchData fetches campaign data for Google.
func (g *GoogleFetcher) FetchData() (interface{}, error) {
	// custom logic for Google campaigns.
	return nil, nil
}

// FetchData fetches campaign data for LinkedIn.
func (l *LinkedInFetcher) FetchData() (interface{}, error) {
	// custom logic for LinkedIn campaigns.
	return nil, nil
}

// FetchData fetches campaign data for TikTok.
func (t *TikTokFetcher) FetchData() (interface{}, error) {
	// custom logic for TikTok campaigns.
	return nil, nil
}

// FetchData fetches campaign data for Meta.
func (m *MetaFetcher) FetchData() (interface{}, error) {
	// custom logic for Meta campaigns.
	return nil, nil
}

func GetCampaignDataFetcher(platform string) (CampaignDataFetcher, error) {
	switch platform {
	case "meta":
		return &MetaFetcher{}, nil
	case "google":
		return &GoogleFetcher{}, nil
	case "linkedin":
		return &LinkedInFetcher{}, nil
	case "tiktok":
		return &TikTokFetcher{}, nil
	default:
		return nil, errors.New("unsupported platform")
	}
}
