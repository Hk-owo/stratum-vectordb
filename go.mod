module stratum

go 1.24

replace google.golang.org/grpc => github.com/grpc/grpc-go v1.69.4

replace google.golang.org/protobuf => github.com/protocolbuffers/protobuf-go v1.34.2

replace go.uber.org/zap => github.com/uber-go/zap v1.28.0

replace github.com/bits-and-blooms/bloom/v3 => github.com/bits-and-blooms/bloom/v3 v3.7.1

replace golang.org/x/net => github.com/golang/net v0.30.0

replace golang.org/x/sys => github.com/golang/sys v0.26.0

replace google.golang.org/genproto/googleapis/rpc => github.com/googleapis/go-genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53

replace golang.org/x/text => github.com/golang/text v0.19.0

require (
	github.com/bits-and-blooms/bloom/v3 v3.7.1
	github.com/cockroachdb/pebble v1.1.5
	go.uber.org/zap v1.28.0
	google.golang.org/grpc v1.69.4
	google.golang.org/protobuf v1.35.1
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240606204812-0bbfbd93a7ce // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/exp v0.0.0-20230626212559-97b1e661b5df // indirect
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
)

replace go.uber.org/multierr => github.com/uber-go/multierr v1.10.0

replace golang.org/x/tools => github.com/golang/tools v0.25.0

replace golang.org/x/sync => github.com/golang/sync v0.9.0

replace golang.org/x/crypto => github.com/golang/crypto v0.28.0

replace golang.org/x/term => github.com/golang/term v0.25.0

replace golang.org/x/mod => github.com/golang/mod v0.17.0

replace golang.org/x/telemetry => github.com/golang/telemetry v0.0.0-20240521205824-bda55230c457

replace golang.org/x/exp => github.com/golang/exp v0.0.0-20230626212559-97b1e661b5df
