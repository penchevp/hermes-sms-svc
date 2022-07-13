package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
	"hermes-sms-svc/db"
	"hermes-sms-svc/sms"
	"os"
	"time"
)

const applicationGroupID = "hermes-sms-svc"
const smsNotificationChannelID = "5cbd9281-f056-48de-80f5-0e4f0d882ce8"

type App struct {
	ctx       context.Context
	smsConfig sms.Config
}

type ConnectJson[T any] struct {
	Before *T `json:"before"`
	After  *T `json:"after"`
}

type Customer struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

type CustomerNotificationChannel struct {
	CustomerID                   uuid.UUID `json:"customer_id"`
	ContactCustomer              bool      `json:"contact_customer"`
	NotificationChannelTypeID    uuid.UUID `json:"notification_channel_type_id"`
	NotificationChannelLookupKey string    `json:"notification_channel_lookup_key"`
}

type Notification struct {
	Text string `json:"text"`
}

type NotificationModel struct {
	Text        string `json:"text"`
	PhoneNumber string `json:"phone_number"`
}

func (app *App) Initialize(ctx context.Context, dbConfig db.Config, smsConfig sms.Config) {
	err := db.InitialiseConnection(dbConfig)
	if err != nil {
		log.Err(err).
			Str("Host", dbConfig.Host).
			Int32("Port", dbConfig.Port).
			Str("User", dbConfig.User).
			Str("Catalog", dbConfig.Catalog).
			Msg("Could not initialise a connection to the database")

		os.Exit(1)
	}

	app.ctx = ctx
	app.smsConfig = smsConfig
}

func (app *App) Run() {
	customerFetchMessageChan := make(chan kafka.Message, 0)
	customerCommitMessageChan := make(chan kafka.Message, 0)
	customerNotificationChannelsFetchMessageChan := make(chan kafka.Message, 0)
	customerNotificationChannelsCommitMessageChan := make(chan kafka.Message, 0)
	notificationsFetchMessageChan := make(chan kafka.Message, 0)
	notificationsCommitMessageChan := make(chan kafka.Message, 0)
	retryQueueFetchMessageChan := make(chan kafka.Message, 0)
	retryQueueCommitMessageChan := make(chan kafka.Message, 0)

	// sync data into own db
	go readKafka(app.ctx, withTopic(standardKafkaConfig(), "hermes.public.customers"), customerFetchMessageChan, customerCommitMessageChan)
	go readKafka(app.ctx, withTopic(standardKafkaConfig(), "hermes.public.customer_notification_channels"), customerNotificationChannelsFetchMessageChan, customerNotificationChannelsCommitMessageChan)

	// support retry for failed delivery
	go readKafka(app.ctx, withTopic(standardKafkaConfig(), "hermes.sms.retry-queue"), retryQueueFetchMessageChan, retryQueueCommitMessageChan)

	// process any new notifications within the consumer group ignoring the past
	go readKafka(app.ctx, withOffset(withTopic(standardKafkaConfig(), "hermes.public.notifications"), kafka.LastOffset), notificationsFetchMessageChan, notificationsCommitMessageChan)

	go func() {
		dbConn, err := db.GetConnection()
		defer db.CloseConnection()

		if err != nil {
			log.Err(err).
				Msg("Could not retrieve connection to the database")

			return
		}
		for {
			select {
			case <-app.ctx.Done():
				{
					return
				}
			case m := <-customerFetchMessageChan:
				{
					var connectJson ConnectJson[Customer]

					err = json.Unmarshal(m.Value, &connectJson)
					if err != nil {
						continue
					}

					err = customerProcessor(dbConn, connectJson)
					if err == nil {
						customerCommitMessageChan <- m
					}
				}
			case m := <-customerNotificationChannelsFetchMessageChan:
				{
					var connectJson ConnectJson[CustomerNotificationChannel]

					err = json.Unmarshal(m.Value, &connectJson)
					if err != nil {
						continue
					}

					err = customerNotificationsChannelsProcessor(dbConn, connectJson)
					if err == nil {
						customerNotificationChannelsCommitMessageChan <- m
					}
				}
			case m := <-notificationsFetchMessageChan:
				{
					var connectJson ConnectJson[Notification]

					err = json.Unmarshal(m.Value, &connectJson)
					if err != nil {
						continue
					}

					err = notificationsProcessor(app.ctx, app.smsConfig, dbConn, connectJson)
					if err == nil {
						notificationsCommitMessageChan <- m
					}
				}
			case m := <-retryQueueFetchMessageChan:
				{
					var model NotificationModel

					err = json.Unmarshal(m.Value, &model)
					if err != nil {
						continue
					}

					err = retryNotificationsProcessor(app.smsConfig, model)
					if err == nil {
						retryQueueCommitMessageChan <- m
					}
				}
			}
		}
	}()
}

func customerProcessor(dbConn *gorm.DB, connectJson ConnectJson[Customer]) error {
	if connectJson.After != nil {
		var existingCustomer db.Customer

		existsResult := dbConn.Where("ID = ?", connectJson.After.ID.String()).First(&existingCustomer)
		if existsResult.Error != nil && !errors.Is(existsResult.Error, gorm.ErrRecordNotFound) {
			log.Err(existsResult.Error)
			return existsResult.Error
		}

		if existsResult.RowsAffected > 0 {
			existingCustomer.Name = connectJson.After.Name
			dbConn.Save(&existingCustomer)
		} else {
			dbConn.Create(db.Customer{ID: connectJson.After.ID, Name: connectJson.After.Name})
		}
	} else if connectJson.Before != nil {
		dbConn.Delete(connectJson.Before)
	}

	return nil
}

func customerNotificationsChannelsProcessor(dbConn *gorm.DB, connectJson ConnectJson[CustomerNotificationChannel]) error {
	smsNotificationChannelUuid, _ := uuid.Parse(smsNotificationChannelID)

	if connectJson.After != nil {
		if connectJson.After.NotificationChannelTypeID != smsNotificationChannelUuid {
			return nil
		}

		var existingCustomer db.Customer

		existsResult := dbConn.Where("ID = ?", connectJson.After.CustomerID.String()).First(&existingCustomer)
		if existsResult.Error != nil && !errors.Is(existsResult.Error, gorm.ErrRecordNotFound) {
			log.Err(existsResult.Error)
			return existsResult.Error
		}

		if existsResult.RowsAffected > 0 {
			existingCustomer.ContactCustomer = connectJson.After.ContactCustomer
			existingCustomer.PhoneNumber = connectJson.After.NotificationChannelLookupKey
			dbConn.Save(&existingCustomer)
		}
	}

	return nil
}

func notificationsProcessor(ctx context.Context, smsConfig sms.Config, dbConn *gorm.DB, connectJson ConnectJson[Notification]) error {
	if connectJson.After != nil && len(connectJson.After.Text) > 0 {
		var customers []db.Customer
		if dbResult := dbConn.Where("contact_customer = true").Find(&customers); dbResult.Error != nil {
			log.Err(dbResult.Error).
				Msg("Error while fetching from database")

			return dbResult.Error
		}

		for _, customer := range customers {
			go func() {
				err := sms.Send(smsConfig, customer.PhoneNumber, connectJson.After.Text)
				if err != nil {
					w := &kafka.Writer{
						Addr:                   kafka.TCP("localhost:29092"),
						Topic:                  "hermes.sms.retry-queue",
						AllowAutoTopicCreation: true,
					}

					notificationModel := NotificationModel{PhoneNumber: customer.PhoneNumber, Text: connectJson.After.Text}
					notificationModelBytes, _ := json.Marshal(notificationModel)

					err := w.WriteMessages(ctx,
						kafka.Message{
							Value: notificationModelBytes,
						},
					)

					if err != nil {
						log.Err(err).
							Msg("Unable to insert failed notification into retry queue")
					}
				}
			}()

			// artificial delay so AWS won't cut us off
			time.Sleep(1 * time.Second)
		}
	}

	return nil
}

func retryNotificationsProcessor(mailerConfig sms.Config, model NotificationModel) error {
	if len(model.Text) > 0 && len(model.PhoneNumber) > 0 {
		err := sms.Send(mailerConfig, model.PhoneNumber, model.Text)
		if err != nil {
			log.Err(err).
				Msg("Unable to process a notification from the retry queue")

			return err
		}
	}

	return nil
}
