# Librescoot Redis IPC Library

Redis-based IPC library for Go with type-safe generics, functional options, and Librescoot-specific patterns for hash-based state management.

Part of the [Librescoot](https://librescoot.org/) open-source platform.

## Features

- **Generics**: Type-safe subscriptions and queue handlers
- **Functional Options**: Flexible client configuration
- **Connection Callbacks**: React to connect/disconnect events
- **Context-Free API**: Uses client's internal context by default
- **Graceful Shutdown**: Wait for handlers to complete
- **Hash State Pattern**: Atomic HSET + PUBLISH with change detection
- **Fault Set Management**: Redis SET with pub/sub notifications
- **Stream Publishing**: XADD with configurable max length
- **Stream Consumption**: XREAD with optional consumer groups
- **Transaction Builder**: Atomic MULTI/EXEC across HashPublisher, FaultSet, and StreamPublisher
- **Multiplexed Connections**: All subscriptions share one pub/sub connection, all queues share one BRPOP loop

## Installation

```bash
go get github.com/librescoot/redis-ipc
```

## Quick Start

```go
import ipc "github.com/librescoot/redis-ipc"

// Create client with options
client, err := ipc.New(
    ipc.WithAddress("localhost"),
    ipc.WithPort(6379),
    ipc.WithOnConnect(func() { log.Println("connected") }),
    ipc.WithOnDisconnect(func(err error) { log.Println("disconnected") }),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

## Typed Subscriptions

```go
type VehicleState struct {
    State   string `json:"state"`
    Speed   int    `json:"speed"`
}

// Subscribe with automatic JSON decoding
sub, err := ipc.Subscribe(client, "vehicle:events", func(msg VehicleState) error {
    log.Printf("State: %s, Speed: %d", msg.State, msg.Speed)
    return nil
})
defer sub.Unsubscribe()

// Publish typed messages
ipc.PublishTyped(client, "vehicle:events", VehicleState{State: "ready", Speed: 0})
```

## Queue Processing

```go
type Command struct {
    Action string `json:"action"`
}

// Handle queue items with automatic restart on errors
handler := ipc.HandleRequests(client, "scooter:commands", func(cmd Command) error {
    log.Printf("Command: %s", cmd.Action)
    return nil
})
defer handler.Stop()

// Send to queue
ipc.SendRequest(client, "scooter:commands", Command{Action: "unlock"})
```

## RPC: Call / HandleCalls

Synchronous request/response over Redis. Request leg uses a list (LPUSH/BRPOP) so requests survive brief server restarts; reply leg uses pub/sub (PUBLISH/SUBSCRIBE) so sub-second timeouts work and there's no per-call key bookkeeping.

```go
type SetProfile struct {
    Profile string `json:"profile"`
}
type ProfileApplied struct {
    Profile string `json:"profile"`
    OK      bool   `json:"ok"`
}

// Server side
srv := ipc.HandleCalls[SetProfile, ProfileApplied](client, "motion:rpc:set-profile",
    func(req SetProfile) (ProfileApplied, error) {
        if err := chip.applyProfile(req.Profile); err != nil {
            return ProfileApplied{}, err
        }
        return ProfileApplied{Profile: req.Profile, OK: true}, nil
    },
    ipc.WithCallConcurrency(4), // optional, default 4
)
defer srv.Stop()

// Client side
resp, err := ipc.Call[SetProfile, ProfileApplied](client, "motion:rpc:set-profile",
    SetProfile{Profile: "armed-hibernation"},
    1*time.Second)
if errors.Is(err, ipc.ErrCallTimeout) {
    // server didn't reply in time
} else if ipc.IsCallError(err) {
    // server-side handler returned an error
}
```

Behaviour notes:
- Request envelopes carry a deadline; servers drop requests whose deadline has already passed (caller already gave up).
- If no server is registered, the request sits in the list. When a server starts, it picks the request up — but if its deadline has expired, it's dropped without a reply. The caller hits `ErrCallTimeout`.
- `Stop()` waits for in-flight handlers to finish before returning.

### CallServer: many methods, one channel

`HandleCalls` is one Redis key per method, which spreads a service's RPC surface across a pile of list keys. `CallServer` is the multi-method shape: one shared request channel per service, the server dispatches by `method` field on the envelope.

```go
// Server side
srv := ipc.NewCallServer(client, "motion:rpc")
ipc.RegisterCall[GetCalReq, CalResp](srv, "get-calibration", handleGetCal)
ipc.RegisterCall[PrepHibReq, PrepHibResp](srv, "prepare-hibernation", handlePrepHib)
ipc.RegisterCall[Empty, OK](srv, "clear-latch", handleClearLatch)
srv.Start()
defer srv.Stop()

// Client side
resp, err := ipc.CallMethod[Req, Resp](client, "motion:rpc", "get-calibration", req, 1*time.Second)
```

- `RegisterCall` panics if called after `Start`, or for a duplicate method name — both are programmer errors and silent ignore would be worse than crashing in dev.
- Unknown method on the server returns a `*CallError` with `Msg = "unknown method: X"` to the caller.
- `WithCallServerConcurrency(n)` caps in-flight handler invocations across all methods on the server (default 4).
- Wire-compatible with the per-method `Call` clients: the envelope's `method` field is optional. `Call`/`HandleCalls` and `CallMethod`/`CallServer` can coexist on different channels.

## Librescoot Hash Pattern

The Librescoot pattern stores state in Redis hashes and notifies via pub/sub:
- Publisher: `HSET vehicle state "ready"` → `PUBLISH vehicle "state"`
- Consumer: `SUBSCRIBE vehicle` → receives `"state"` → `HGET vehicle state`

### HashPublisher

```go
// Create publisher for "vehicle" hash (publishes to "vehicle" channel)
vehicle := client.NewHashPublisher("vehicle")

// Or with a custom channel name
vehicle := client.NewHashPublisherWithChannel("vehicle", "state")

// Set field and publish atomically
vehicle.Set("state", "ready")

// Only publish if value changed
changed, _ := vehicle.SetIfChanged("state", "ready")

// Batch updates with selective publishing
vehicle.SetManyIfChanged(map[string]any{
    "state":      "parked",
    "kickstand":  "down",
    "brake:left": "off",
})

// Set with automatic timestamp field
vehicle.SetWithTimestamp("state", "ready")
// Sets both "state" and "state:timestamp"

// Delete a single field
vehicle.Delete("old-field")

// Clear entire hash
vehicle.Clear()

// Atomic replace: DEL + HMSET + PUBLISH
vehicle.ReplaceAll(map[string]any{
    "state": "ready",
    "speed": 0,
})

// Set without publishing (silent update)
vehicle.Set("internal-state", "value", ipc.NoPublish())
vehicle.SetMany(fields, ipc.NoPublish())

// Batch update with single notification
vehicle.SetManyPublishOne(map[string]any{
    "lat": "52.520",
    "lon": "13.405",
}, "location")  // Publishes only "location", not each field
```

### HashWatcher

```go
// Create watcher for "battery:0" hash
watcher := client.NewHashWatcher("battery:0")

// Or with a custom channel name
watcher := client.NewHashWatcherWithChannel("battery:0", "battery")

// Register field-specific handlers
watcher.OnField("state", func(value string) error {
    log.Printf("Battery state: %s", value)
    return nil
})

watcher.OnField("charge", func(value string) error {
    log.Printf("Battery charge: %s%%", value)
    return nil
})

// Catch-all for unhandled fields
watcher.OnAny(func(field, value string) error {
    log.Printf("%s = %s", field, value)
    return nil
})

// Typed handler with automatic JSON decoding
ipc.OnFieldTyped(watcher, "config", func(cfg Config) error {
    return applyConfig(cfg)
})

// Start watching
watcher.Start()
defer watcher.Stop()

// Fetch initial state
all, _ := watcher.FetchAll()

// Fetch a single field
state, _ := watcher.Fetch("state")
```

#### StartWithSync

Subscribe first, fetch current state, then process messages. This avoids race conditions: `StartWithSync` returns only once Redis has confirmed the SUBSCRIBE, and anything published while the initial `HGETALL` runs is buffered and delivered afterwards.

```go
watcher := client.NewHashWatcher("vehicle")
watcher.OnField("state", handleState)

// Subscribes, fetches HGETALL, calls handlers, then processes messages
watcher.StartWithSync()
```

#### Debouncing

Coalesce rapid updates - only the last value is passed to handlers after the quiet period:

```go
watcher := client.NewHashWatcher("vehicle")
watcher.SetDebounce(500 * time.Millisecond)  // Wait 500ms after last update
watcher.OnField("speed", handleSpeed)        // Called once after rapid changes settle
watcher.Start()
```

### FaultSet

```go
// Manage fault codes in a Redis SET with pub/sub notification
faults := client.NewFaultSet("battery:0:fault", "battery:0", "fault")

faults.Add(35)     // SADD + PUBLISH
faults.Remove(35)  // SREM + PUBLISH
faults.Has(35)     // SISMEMBER
faults.All()       // SMEMBERS
faults.Clear()     // DEL + PUBLISH
```

## Redis Streams

### StreamPublisher

Publish events to a Redis stream using XADD:

```go
// Create publisher with default max length (1000)
stream := client.NewStreamPublisher("events:faults")

// Or with custom max length
stream := client.NewStreamPublisher("events:faults", ipc.WithMaxLen(5000))

// Publish a map
id, err := stream.Add(map[string]any{
    "group":       "battery:0",
    "code":        "35",
    "description": "NFC Reader Error",
})

// Publish a typed struct (JSON-encoded to fields)
type FaultEvent struct {
    Group       string `json:"group"`
    Code        int    `json:"code"`
    Description string `json:"description"`
}

id, err := ipc.StreamAdd(stream, FaultEvent{
    Group:       "battery:0",
    Code:        35,
    Description: "NFC Reader Error",
})
```

### StreamConsumer

Consume stream messages using XREAD:

```go
// Create consumer
consumer := client.NewStreamConsumer("events:faults",
    ipc.WithBlockTimeout(1 * time.Second),
)

// Set handler
consumer.Handle(func(id string, values map[string]string) error {
    log.Printf("Fault %s: group=%s code=%s", id, values["group"], values["code"])
    return nil
})

// Start from beginning ("0") or only new messages ("$")
consumer.Start("0")

// Or use typed handler
ipc.StreamHandle(consumer, func(id string, evt FaultEvent) error {
    log.Printf("Fault: %+v", evt)
    return nil
})
consumer.Start("$")
```

### Consumer Groups

For multi-instance consumption with acknowledgment:

```go
consumer := client.NewStreamConsumer("events:faults",
    ipc.WithBlockTimeout(1 * time.Second),
    ipc.WithConsumerGroup("uplink-service", "instance-1"),
)

consumer.Handle(handler)

// ">" means only undelivered messages to this group
consumer.Start(">")
```

## Transaction Builder

`Tx` combines operations from `HashPublisher`, `FaultSet`, and `StreamPublisher` into a single atomic MULTI/EXEC pipeline. Hash caches are only updated after a successful `Exec`.

```go
battery := client.NewHashPublisher("battery:0")
faults := client.NewFaultSet("battery:0:fault", "battery:0", "fault")
events := client.NewStreamPublisher("events:faults")

// All operations execute atomically
tx := client.NewTx()
changed := tx.HashSetManyIfChanged(battery, map[string]any{
    "state": "fault",
    "code":  "35",
})
tx.FaultAdd(faults, 35)
tx.StreamAdd(events, map[string]any{"event": "fault_detected", "code": "35"})

if err := tx.Exec(); err != nil {
    // Redis error: nothing committed, hash cache unchanged
}
// On success: hash fields written, fault added, stream entry appended, cache updated

// Batch fault add/remove with a single PUBLISH
tx2 := client.NewTx()
tx2.FaultUpdate(faults, []int{36, 37}, []int{35}) // add 36+37, remove 35
tx2.Exec()

// Use a specific context (e.g., for timeout)
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
tx3 := client.NewTxWithContext(ctx)
tx3.HashSetManyIfChanged(battery, fields)
tx3.Exec()
```

## Message Router

For JSON envelope-based routing (`{"type": "...", "data": {...}}`):

```go
router := client.NewRouter("events")

ipc.Handle(router, "state", func(s StateMsg) error { ... })
ipc.Handle(router, "error", func(e ErrorMsg) error { ... })

router.Start()
defer router.Stop()

// Publish routed messages
ipc.PublishRouted(client, "events", "state", StateMsg{...})
```

## Connection Model

A `Client` holds far fewer Redis connections than it has consumers.

- **One pub/sub connection per client.** Every `HashWatcher`, `Subscribe[T]` and `Router` registers its channel on a shared subscriber. Ten watchers cost one connection and one SUBSCRIBE per channel, not ten connections. go-redis dedicates a connection to each `PubSub` object, so the naive shape also cost a health-check ping every 3s per watcher.
- **One BRPOP loop per client.** Every `HandleRequests`, `HandleCalls` and `CallServer` joins a shared blocking pop over all registered keys. BRPOP reports which key popped, so the mux routes by key name. The key order rotates each iteration, so a busy queue cannot starve a quiet one.
- **Isolation is preserved.** Each subscriber and each queue gets its own goroutine and buffered backlog, so a slow handler stalls only its own channel or queue.

Nothing about this is visible in the API: `Start()`, `Stop()`, `HandleRequests` and friends behave as before. Two consequences worth knowing:

- `WithPoolSize` no longer needs padding for blocking consumers. A service with eight command queues used to need `WithPoolSize(12)` just so its BRPOPs wouldn't exhaust the pool; the default of 3 is now fine.
- `Stop()` on one watcher only unsubscribes the underlying channel if no other watcher in the process still wants it.

## Configuration Options

```go
client, err := ipc.New(
    ipc.WithURL("redis://localhost:6379"),  // Or just "localhost:6379" or "localhost"
    // Or use separate address/port:
    // ipc.WithAddress("localhost"),
    // ipc.WithPort(6379),
    ipc.WithRetryInterval(5 * time.Second),
    ipc.WithMaxRetries(3),
    ipc.WithPoolSize(3),  // Blocking consumers share one connection, so the default is usually enough
    ipc.WithDialTimeout(2 * time.Second),
    ipc.WithLogger(slog.Default()),
    ipc.WithCodec(ipc.JSONCodec{}),  // or ipc.StringCodec{}
    ipc.WithOnConnect(func() { ... }),
    ipc.WithOnDisconnect(func(err error) { ... }),
)
```

## Direct Redis Operations

All operations use the client's internal context:

```go
// Strings
client.Get("key")
client.Set("key", "value", 0)
client.Incr("counter")

// Hashes
client.HGet("hash", "field")
client.HSet("hash", "field", "value")
client.HGetAll("hash")

// Lists
client.LPush("queue", "value")
client.BRPop(time.Second, "queue")

// Pub/Sub
client.Publish("channel", "message")

// Keys
client.Exists("key")
client.Del("key")
client.Expire("key", time.Hour)

// Health check
client.Ping()

// Raw command
client.Do("PING")

// Access underlying go-redis client (requires context)
client.Raw().Scan(ctx, ...)
```

## Best Practices

### Reuse Publishers and Watchers

Create publishers once and store them in your service struct:

```go
// Good: create once, reuse
type Service struct {
    client   *ipc.Client
    powerPub *ipc.HashPublisher
    battPub  *ipc.HashPublisher
}

func NewService(client *ipc.Client) *Service {
    return &Service{
        client:   client,
        powerPub: client.NewHashPublisher("power-manager"),
        battPub:  client.NewHashPublisher("battery:0"),
    }
}

func (s *Service) UpdatePowerState(state string) error {
    return s.powerPub.Set("state", state)
}

// Bad: creates new publisher on every call (wasteful)
func (s *Service) UpdatePowerStateBad(state string) error {
    pub := s.client.NewHashPublisher("power-manager")
    return pub.Set("state", state)
}
```

### Use SetManyIfChanged for Bulk Updates

When updating multiple fields, `SetManyIfChanged` only publishes changed fields:

```go
// Returns list of actually-changed fields (useful for logging)
changed, err := pub.SetManyIfChanged(map[string]any{
    "state":  newState,
    "speed":  newSpeed,
    "charge": newCharge,
})
if len(changed) > 0 {
    log.Printf("Updated fields: %v", changed)
}
```

### Use ReplaceAll for Complete Hash Replacement

When you need to atomically clear and repopulate a hash (e.g., inhibitor lists):

```go
// Atomic: DEL + HMSET + PUBLISH in one transaction
pub.ReplaceAll(map[string]any{
    "inhibitor1": "reason1",
    "inhibitor2": "reason2",
})

// Clear the hash entirely
pub.ReplaceAll(nil)  // or pub.Clear()
```

### Use StartWithSync for Initial State

When you need current values before processing updates:

```go
watcher := client.NewHashWatcher("vehicle")
watcher.OnField("state", handleState)

// StartWithSync: Subscribe → HGETALL → call handlers → process messages
// Ensures no messages are missed during initialization
watcher.StartWithSync()

// vs Start(): Just subscribes, doesn't fetch initial state
// watcher.Start()
```

## License

This project is dual-licensed. The source code is available under the
[GNU Affero General Public License v3.0][agpl-3.0].
The maintainers reserve the right to grant separate licenses for commercial distribution; please contact the maintainers to discuss commercial licensing.

[![AGPL v3][agpl-image]][agpl-3.0]

[agpl-3.0]: https://www.gnu.org/licenses/agpl-3.0.en.html
[agpl-image]: https://www.gnu.org/graphics/agplv3-88x31.png
