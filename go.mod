module github.com/RichardKnop/machinery

go 1.26.0

require (
	cloud.google.com/go/pubsub v1.52.0
	github.com/RichardKnop/logging v0.0.0-20251209231334-9b7145a2bbb1
	github.com/RichardKnop/redsync v1.2.0
	github.com/aws/aws-sdk-go-v2 v1.48.0
	github.com/aws/aws-sdk-go-v2/config v1.33.21
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.21.23
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.74.1
	github.com/aws/aws-sdk-go-v2/service/sqs v1.53.0
	github.com/bradfitz/gomemcache v0.0.0-20260422231931-4d751bb6e37c
	github.com/btcsuite/btcutil v1.0.2
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-redsync/redsync/v4 v4.18.1
	github.com/gomodule/redigo v1.9.3
	github.com/google/uuid v1.6.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/rabbitmq/amqp091-go v1.17.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/spf13/cast v1.10.0
	github.com/stretchr/testify v1.12.1
	github.com/urfave/cli v1.22.17
	go.mongodb.org/mongo-driver v1.17.10
	gopkg.in/yaml.v2 v2.4.0
)

require (
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.24.4 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.3.1 // indirect
	cloud.google.com/go/compute/metadata v0.10.0 // indirect
	cloud.google.com/go/iam v1.15.1 // indirect
	cloud.google.com/go/pubsub/v2 v2.9.0 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.21.3 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.48.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.13.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.14.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.39.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.44.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.53.0 // indirect
	github.com/aws/smithy-go v1.28.10 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/s2a-go v0.1.11 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.25 // indirect
	github.com/googleapis/gax-go/v2 v2.27.0 // indirect
	github.com/klauspost/compress v1.20.5 // indirect
	github.com/montanaflynn/stats v0.14.0 // indirect
	github.com/onsi/gomega v1.27.10 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/xdg-go/scram v1.4.0 // indirect
	github.com/xdg-go/stringprep v1.0.5 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.2 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.72.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.72.0 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/net v0.63.0 // indirect
	golang.org/x/oauth2 v0.38.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	google.golang.org/api v0.316.0 // indirect
	google.golang.org/genproto v0.0.0-20261123191846-1e707ab9fbc2 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261123191846-1e707ab9fbc2 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261123191846-1e707ab9fbc2 // indirect
	google.golang.org/grpc v1.85.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace git.apache.org/thrift.git => github.com/apache/thrift v0.0.0-20180902110319-2566ecd5d999
