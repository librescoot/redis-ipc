# LibreScoot Redis IPC Library

Redis-based IPC library for Go with type-safe generics, functional options, and LibreScoot-specific patterns for hash-based state management.

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

## LibreScoot Hash Pattern

The LibreScoot pattern stores state in Redis hashes and notifies via pub/sub:
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

Subscribe first, fetch current state, then process messages. This avoids race conditions:

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

## Transactions

```go
tx := client.NewTxGroup()
tx.Add("HSET", "vehicle", "state", "ready").
   Add("HSET", "vehicle", "state:timestamp", time.Now().Unix()).
   Add("PUBLISH", "vehicle", "state")

results, err := tx.Exec()
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

## Configuration Options

```go
client, err := ipc.New(
    ipc.WithURL("redis://localhost:6379"),  // Or just "localhost:6379" or "localhost"
    // Or use separate address/port:
    // ipc.WithAddress("localhost"),
    // ipc.WithPort(6379),
    ipc.WithRetryInterval(5 * time.Second),
    ipc.WithMaxRetries(3),
    ipc.WithPoolSize(3),
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

AGPL-3.0, see LICENSE for details.
