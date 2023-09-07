module github.com/RichardKnop/machinery

go 1.15

require (
	cloud.google.com/go v0.76.0 // indirect
	cloud.google.com/go/pubsub v1.10.0
	github.com/RichardKnop/logging v0.0.0-20190827224416-1a693bdd4fae
	github.com/RichardKnop/redsync v1.2.0
	github.com/aws/aws-sdk-go v1.25.8
	github.com/aws/aws-sdk-go-v2 v1.16.16
	github.com/aws/aws-sdk-go-v2/config v1.17.8
	github.com/aws/aws-sdk-go-v2/service/sqs v1.19.10
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/btcsuite/btcutil v1.0.2
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-redsync/redsync/v4 v4.8.2
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/uuid v1.2.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/spf13/cast v1.5.0
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	github.com/stretchr/testify v1.8.1
	github.com/urfave/cli v1.22.1
	go.mongodb.org/mongo-driver v1.11.6
	golang.org/x/net v0.9.0 // indirect
	gopkg.in/yaml.v2 v2.4.0
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
