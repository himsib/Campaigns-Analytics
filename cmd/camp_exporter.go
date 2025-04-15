package cmd

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/DTSL/golang-libraries/kafkautils"
	"github.com/DTSL/golang-libraries/timeutils"
	"github.com/DTSL/golang-libraries/tracingutils"
	"github.com/DTSL/sms-marketing-events/kafkaevents"
	"go.mongodb.org/mongo-driver/mongo"
)

// sample interface
type campaignExporter struct {
	campaignsCSVGenerator interface {
		GenerateCSV(ctx context.Context, args *kafkaevents.SmsExportMessage, fileName string, camp *campaign, emailDB *mongo.Database, config *config) error
	}
	campaignsEventsCSVGenerator interface {
		GenerateEventsCSV(ctx context.Context, args *kafkaevents.SmsExportMessage, fileName string, camp *campaign, config *config, emailDB *mongo.Database) error
	}
	structuredLogger interface {
		Log(context.Context, interface{})
	}
	fileUploader interface {
		UploadFile(ctx context.Context, fileName string) (string, error)
		UploadFileToAWS(ctx context.Context, fileName string) (string, error)
	}
	notificationHandler interface {
		SendNotification(ctx context.Context, fileURL string, args *kafkaevents.SmsExportMessage, camp *campaign, processId int64, config *config) error
	}
	campaignManager interface {
		GetCampaign(ctx context.Context, campaignID int64, emailDB *mongo.Database) (*campaign, error)
	}
	processManager interface {
		Generate(ctx context.Context, fileURL string, camp *campaign, args *kafkaevents.SmsExportMessage, emailDB *mongo.Database) (int64, error)
		updateProcessById(ctx context.Context, emailDB *mongo.Database, processId int64, fileURL string) error
	}
	mongoDBClientConnectionGetter mongoevents.MongoDBClientConnection
	configManager                 interface {
		GetConfig(ctx context.Context, emailDB *mongo.Database) (*config, error)
	}
	notifyWebhookHandler interface {
		sendNotifyWebhook(ctx context.Context, args *kafkaevents.SmsExportMessage, fileUrl string) error
	}
}

func (s *campaignExporter) process(ctx context.Context, expMsg *kafkaevents.SmsExportMessage) error {
	var err error
	span, spanFinish := tracingutils.StartRootSpan(&ctx, "export_data_consumer.process", &err)
	defer spanFinish()
	span.SetTag("message", fmt.Sprintf("%v", expMsg))

	sl := &structuredLog{
		Time:       timeutils.Now().UTC(),
		CampaignID: expMsg.CampaignID,
		ClientID:   expMsg.OrganizationID,
	}
	defer func() {
		sl.TimeProcessing = int64(timeutils.Now().UTC().Sub(sl.Time).Seconds())
		s.writeLog(ctx, sl, err)
	}()
	args := expMsg
	emailDB, err := s.getClientEmailingDB(ctx, expMsg.OrganizationID)
	if err != nil {
		return err
	}
	config, err := s.configManager.GetConfig(ctx, emailDB)
	if err != nil {
		return err
	}
	camp, err := s.getCampaign(ctx, args, emailDB)
	if err != nil {
		return err
	}
	if err = s.validateCampaignReportData(camp); err != nil {
		err = kafkautils.ConsumerErrorWithHandler(err, kafkautils.ConsumerDiscard)
		return err
	}
	fileName, fileURL, err := s.generateAndUploadCSVToAWS(ctx, args, camp, emailDB, config)
	if err != nil {
		return err
	}
	processId, err := s.updateCSVProcess(ctx, emailDB, args, camp, fileURL)
	if err != nil {
		return err
	}

	err = s.notificationHandler.SendNotification(ctx, fileURL, args, camp, processId, config)
	if err != nil {
		return err
	}

	err = os.Remove(fileName)
	if err != nil {
		log.Println("error in removing file", err)
	}
	return nil
}
