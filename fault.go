package redis_ipc

import (
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// FaultReporter couples a fault set, the events:faults stream, and the
// per-group pubsub notification into a single primitive. Raise and Clear
// run as a server-side EVAL transaction: SADD/SREM, branch on the change
// count, and only emit XADD + PUBLISH on a real state transition. Set and
// stream cannot drift, even across process restarts or crashing callers.
type FaultReporter struct {
	client  *Client
	group   string
	setKey  string
	channel string
	payload string
	stream  string
	maxLen  int64
}

type faultReporterOptions struct {
	setKey  string
	channel string
	payload string
	stream  string
	maxLen  int64
}

// FaultReporterOption configures a FaultReporter.
type FaultReporterOption func(*faultReporterOptions)

// WithFaultSetKey overrides the fault set key. Default: "<group>:fault".
func WithFaultSetKey(key string) FaultReporterOption {
	return func(o *faultReporterOptions) { o.setKey = key }
}

// WithFaultChannel overrides the pubsub channel. Default: "<group>".
func WithFaultChannel(ch string) FaultReporterOption {
	return func(o *faultReporterOptions) { o.channel = ch }
}

// WithFaultPayload overrides the pubsub payload. Default: "fault".
func WithFaultPayload(p string) FaultReporterOption {
	return func(o *faultReporterOptions) { o.payload = p }
}

// WithFaultStream overrides the events stream name. Default: "events:faults".
func WithFaultStream(s string) FaultReporterOption {
	return func(o *faultReporterOptions) { o.stream = s }
}

// WithFaultMaxLen overrides the stream MAXLEN. Default: 1000 (approximate).
func WithFaultMaxLen(n int64) FaultReporterOption {
	return func(o *faultReporterOptions) { o.maxLen = n }
}

// NewFaultReporter creates a FaultReporter for the given group. Defaults
// follow the Librescoot convention:
//
//	set key:  "<group>:fault"
//	channel:  "<group>"
//	payload:  "fault"
//	stream:   "events:faults"
//	MAXLEN:   ~1000
func (c *Client) NewFaultReporter(group string, opts ...FaultReporterOption) *FaultReporter {
	o := &faultReporterOptions{
		setKey:  group + ":fault",
		channel: group,
		payload: "fault",
		stream:  "events:faults",
		maxLen:  1000,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &FaultReporter{
		client:  c,
		group:   group,
		setKey:  o.setKey,
		channel: o.channel,
		payload: o.payload,
		stream:  o.stream,
		maxLen:  o.maxLen,
	}
}

// raiseFaultScript: idempotent SADD + (if changed) XADD + PUBLISH.
//
//	KEYS[1] = set key, KEYS[2] = stream name, KEYS[3] = channel
//	ARGV[1] = code, ARGV[2] = group, ARGV[3] = description,
//	ARGV[4] = MAXLEN,    ARGV[5] = pubsub payload
var raiseFaultScript = redis.NewScript(`
local added = redis.call("SADD", KEYS[1], ARGV[1])
if added == 1 then
  redis.call("XADD", KEYS[2], "MAXLEN", "~", ARGV[4], "*",
             "group", ARGV[2], "code", ARGV[1], "description", ARGV[3])
  redis.call("PUBLISH", KEYS[3], ARGV[5])
end
return added
`)

// clearFaultScript: idempotent SREM + (if changed) XADD with -code + PUBLISH.
//
//	KEYS[1] = set key, KEYS[2] = stream name, KEYS[3] = channel
//	ARGV[1] = code, ARGV[2] = group,
//	ARGV[3] = MAXLEN, ARGV[4] = pubsub payload
var clearFaultScript = redis.NewScript(`
local removed = redis.call("SREM", KEYS[1], ARGV[1])
if removed == 1 then
  redis.call("XADD", KEYS[2], "MAXLEN", "~", ARGV[3], "*",
             "group", ARGV[2], "code", "-" .. ARGV[1])
  redis.call("PUBLISH", KEYS[3], ARGV[4])
end
return removed
`)

// Raise marks code as present. Idempotent: returns nil and emits no stream
// entry if the code is already in the set.
func (fr *FaultReporter) Raise(code int, description string) error {
	ctx := fr.client.Context()
	_, err := raiseFaultScript.Run(ctx, fr.client.redis,
		[]string{fr.setKey, fr.stream, fr.channel},
		code, fr.group, description, fr.maxLen, fr.payload,
	).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("fault raise: %w", err)
	}
	return nil
}

// Clear marks code as absent. Idempotent: returns nil and emits no stream
// entry if the code is not in the set.
func (fr *FaultReporter) Clear(code int) error {
	ctx := fr.client.Context()
	_, err := clearFaultScript.Run(ctx, fr.client.redis,
		[]string{fr.setKey, fr.stream, fr.channel},
		code, fr.group, fr.maxLen, fr.payload,
	).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("fault clear: %w", err)
	}
	return nil
}

// Has reports whether the given fault code is currently in the set.
func (fr *FaultReporter) Has(code int) (bool, error) {
	return fr.client.redis.SIsMember(fr.client.Context(), fr.setKey, code).Result()
}

// All returns all fault codes currently in the set, parsed as ints.
// Returns an error if any member can't be parsed as an integer.
func (fr *FaultReporter) All() ([]int, error) {
	members, err := fr.client.redis.SMembers(fr.client.Context(), fr.setKey).Result()
	if err != nil {
		return nil, err
	}
	codes := make([]int, 0, len(members))
	for _, m := range members {
		c, err := strconv.Atoi(m)
		if err != nil {
			return nil, fmt.Errorf("non-integer fault code %q in set %s: %w", m, fr.setKey, err)
		}
		codes = append(codes, c)
	}
	return codes, nil
}

// Group returns the group name.
func (fr *FaultReporter) Group() string { return fr.group }
