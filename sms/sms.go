package sms

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/rs/zerolog/log"
)

type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

func Send(config Config, recipient string, body string) error {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, ""),
	})

	if err != nil {
		log.Err(err).Msg("Could not create AWS session")
		return err
	}

	svc := sns.New(sess)

	result, err := svc.Publish(&sns.PublishInput{
		Message:     &body,
		PhoneNumber: &recipient,
	})

	if err != nil {
		log.Err(err)
		return err
	}

	// can save the resulting message id as a receipt, maybe send to a sink connector in elastic?
	fmt.Println(result)

	return nil
}
