package cmd

import (
	"github.com/DTSL/golang-libraries/closeutils"
	"github.com/DTSL/golang-libraries/kafkautils"
	"github.com/pkg/errors"
)

type diContainer struct {
	kafka *kafkaevents.DIContainer
}

func (d *diContainer) kafkaProducer() (*kafkautils.SimpleProducer, error) {
	producer, err := d.kafka.ProducerSingle()
	if err != nil {
		return nil, errors.Wrap(err, "newProducerSingle")
	}
	return producer, nil
}

func newDIContainer(flg *flags) (*diContainer, closeutils.WithOnErr) {
	dic := &diContainer{
		flags: flg,
	}
	dic.kafka = kafkaevents.NewDIContainer(flg)
	return dic, func() {
		if dic.kafka != nil {
			dic.kafka.Close()
		}
	}
}
