package model

import adapter "worker-template/adapter/kafka"

type Config struct {
	Adapter Adapter `yaml:"adapter"`
}

type Adapter struct {
	Kafka Kafka `yaml:"kafka"`
}

type Kafka struct {
	Topic    TopicConfig            `yaml:"topic"`
	Config   adapter.Config         `yaml:"config"`
	Producer adapter.ProducerConfig `yaml:"producer"`
	Consumer adapter.ConsumerConfig `yaml:"consumer"`
}

type TopicConfig struct {
	TopicEvent    string `yaml:"topic_event"`
	ConsumerEvent string `yaml:"consumer_event"`
}
