package main

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/DTSL/golang-libraries/errorhandle"
	"github.com/DTSL/golang-libraries/errors"
	"github.com/DTSL/golang-libraries/jsontracing"
	"github.com/DTSL/golang-libraries/kafkautils"
	"github.com/DTSL/sms-marketing-events/kafkaevents"
	"github.com/segmentio/kafka-go"
)

const (
	maxRetryCnt = 3
)

func runKafka(ctx context.Context, dic *diContainer) error {
	readerCfg, err := getKafkaReaderConfig(dic)
	if err != nil {
		return errors.Wrap(err, "get reader config")
	}
	pr, err := dic.kafkaProcessor()
	if err != nil {
		return errors.Wrap(err, "get processor")
	}
	retryProducer := &kafkautils.DiscardProducer{
		HeaderProducer: kafkautils.HeaderProducer{
			Producer: pr.kafkaProducer.Produce,
			Topic:    kafkaevents.SmsReportIngestTopic,
		},
		GroupName: appName,
	}
	smsExportDeadProducer := &kafkautils.DiscardProducer{
		HeaderProducer: kafkautils.HeaderProducer{
			Producer: pr.kafkaProducer.Produce,
			Topic:    kafkaevents.SmsReportIngestDeadTopic,
		},
		GroupName: appName,
	}
	kafkautils.RunConsumers(ctx, readerCfg, pr.process, dic.flags.consumers, retryProducer.Produce, smsExportDeadProducer.Produce, errorhandle.HandleDefault)

	return nil
}

func getKafkaReaderConfig(dic *diContainer) (kafka.ReaderConfig, error) {
	cfg, err := kafkaevents.GetConfig(dic.flags.environment)
	if err != nil {
		return kafka.ReaderConfig{}, errors.Wrap(err, "config")
	}
	readerCfg := cfg.NewReaderConfig()
	readerCfg.Topic = kafkaevents.SmsReportIngestTopic //topic name
	readerCfg.GroupID = appName
	readerCfg.WatchPartitionChanges = true
	readerCfg.ErrorLogger = kafka.LoggerFunc(func(format string, args ...interface{}) {
		log.Printf("Kafka reader error: "+format, args...)
	})
	return readerCfg, nil
}

type kafkaProcessor struct {
	processor         func(ctx context.Context, expMsg *kafkaevents.SmsExportMessage) error
	kafkaProducer     *kafkautils.SimpleProducer
	kafkaWaitProducer *kafkautils.WaitProducer
}

func newKafkaProcessor(dic *diContainer) (*kafkaProcessor, error) {
	exPro, err := dic.exportCampaign()
	if err != nil {
		return nil, errors.Wrap(err, "exportCampaign")
	}

	kafkaProducer, err := dic.kafka.ProducerSingle()
	if err != nil {
		return nil, errors.Wrap(err, "newProducerSingle")
	}

	kafkaWaitProducer, err := dic.kafka.ProducerWait()
	if err != nil {
		return nil, errors.Wrap(err, "newWaitProducerSingle")
	}

	return &kafkaProcessor{
		processor:         exPro.process,
		kafkaProducer:     kafkaProducer,
		kafkaWaitProducer: kafkaWaitProducer,
	}, nil
}

func newKafkaProcessorDIProvider(dic *diContainer) func() (*kafkaProcessor, error) {
	var p *kafkaProcessor
	var mu sync.Mutex
	return func() (_ *kafkaProcessor, err error) {
		mu.Lock()
		defer mu.Unlock()
		if p == nil {
			p, err = newKafkaProcessor(dic)
		}
		return p, err
	}
}

func (p *kafkaProcessor) process(ctx context.Context, kmsgs kafka.Message) error {
	msgs, err := p.decodeMessage(ctx, kmsgs)
	if err != nil {
		err = errors.Wrap(err, "decode message")
		err = kafkautils.ConsumerErrorWithHandler(err, kafkautils.ConsumerDiscard)
		return err
	}
	err = p.processor(ctx, msgs)
	if err != nil {
		err = errors.Wrap(err, "processor")
		if !retriesWithDelays(kmsgs) {
			log.Println("in retriesWithDelays code block.....")
			err = kafkautils.ConsumerErrorWithHandler(err, kafkautils.ConsumerDiscard)
		}
		log.Println("processor err", err)
		return err
	}

	return nil
}

func retriesWithDelays(kmsgs kafka.Message) bool {
	// put message in the dead queue after 3 retries(prevent infinite retries)
	retryCountB, _ := kafkautils.GetHeader(kmsgs.Headers, "discard_retry_count")
	retryCount, _ := strconv.ParseInt(string(retryCountB), 10, 64)
	log.Println("retryCount...", retryCount)
	if retryCount >= maxRetryCnt {
		return false
	}
	log.Println("in retry count...", retryCount)
	initialDelay := 5 * time.Second
	delay := time.Duration(float64(initialDelay) * (float64(retryCount) + 1))
	log.Println("delay time...", delay)
	time.Sleep(delay)
	return true
}

func (p *kafkaProcessor) decodeMessage(ctx context.Context, kmsg kafka.Message) (*kafkaevents.SmsExportMessage, error) {
	msg := &kafkaevents.SmsExportMessage{}

	err := jsontracing.Unmarshal(ctx, kmsg.Value, &msg)
	if err != nil {
		return nil, errors.Wrap(err, "JSON unmarshal")
	}
	if msg == nil {
		return nil, errors.Wrap(err, "no data")
	}
	// Validate required fields
	if msg.OrganizationID == 0 || msg.CampaignID == 0 {
		return nil, errors.New("missing required fields")
	}

	return msg, nil
}
