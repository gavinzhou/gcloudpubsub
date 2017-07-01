// Copyright 2016 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// [START pubsub_quickstart]
// Sample pubsub-quickstart creates a Google Cloud Pub/Sub topic.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	// Imports the Google Cloud Pub/Sub client package.
	"cloud.google.com/go/pubsub"
	"github.com/satori/go.uuid"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type Session struct {
	SessionID string `json:"sessionid"`
	TimeStamp int64  `json:"timestamp"`
}

func main() {
	ctx := context.Background()
	// [START auth]
	proj := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if proj == "" {
		fmt.Fprintf(os.Stderr, "GOOGLE_CLOUD_PROJECT env variable must be set.\n")
		os.Exit(1)
	}

	// Creates a client.
	client, err := pubsub.NewClient(ctx, proj)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	// [END auth]

	// List all the topics from the project.
	fmt.Println("Listing all topics from the project:")
	topics, err := list(client)
	if err != nil {
		log.Fatalf("Failed to list topics: %v", err)
	}
	for _, t := range topics {
		fmt.Println(t)
	}

	const topicName = "example-topic"
	// Create a new topic call mytest-topic
	topic := createTopicIfNotExists(client, topicName)

	// Publish a text message on the created topic.
	for i := 0; i < 10; i++ {
		msgUuid := uuid.NewV4().String()
		// message we want to send to subscriber
		session, _ := json.Marshal(&Session{
			SessionID: msgUuid,
			TimeStamp: time.Now().Unix(),
		})
		// publish message to Cloud Pub/Sub
		_, err = topic.Publish(ctx, &pubsub.Message{
			Data: session,
		}).Get(ctx)
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}

		log.Printf("%s send", msgUuid)
		time.Sleep(1 * time.Second)
	}
}

func createTopicIfNotExists(c *pubsub.Client, topic string) *pubsub.Topic {
	ctx := context.Background()

	// Create a topic to subscribe to.
	t := c.Topic(topic)
	ok, err := t.Exists(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		return t
	}

	t, err = c.CreateTopic(ctx, topic)
	if err != nil {
		log.Fatalf("Failed to create the topic: %v", err)
	}
	return t
}

func create(client *pubsub.Client, topic string) error {
	ctx := context.Background()
	// [START create topic]
	t, err := client.CreateTopic(ctx, topic)
	if err != nil {
		return err
	}
	fmt.Printf("Topic created: %v\n", t)
	// [END create topic]
	return nil
}

func delete(client *pubsub.Client, topic string) error {
	ctx := context.Background()
	// [START delete topic]
	t := client.Topic(topic)
	if err := t.Delete(ctx); err != nil {
		return err
	}
	fmt.Printf("Deleted topic: %v\n", t)
	// [END delete topic]
	return nil
}

func list(client *pubsub.Client) ([]*pubsub.Topic, error) {
	ctx := context.Background()

	var topics []*pubsub.Topic
	it := client.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
	}

	return topics, nil
	// [END list_topics]
}

func publish(client *pubsub.Client, topic, msg string) error {
	ctx := context.Background()
	// [START publish]
	t := client.Topic(topic)
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(msg),
	})

	id, err := result.Get(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Publish a message; msg ID: %v\n", id)
	// [END publish]
	return nil
}
