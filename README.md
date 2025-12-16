# LibreScoot Redis IPC Library

Redis-based IPC library for Go with type-safe generics, functional options, and LibreScoot-specific patterns for hash-based state management.

## Features

- **Generics**: Type-safe subscriptions and queue handlers
- **Functional Options**: Flexible client configuration
- **Connection Callbacks**: React to connect/disconnect events
- **Context Propagation**: All operations accept `context.Context`
- **Graceful Shutdown**: Wait for handlers to complete
- **Hash State Pattern**: Atomic HSET + PUBLISH with change detection
- **Fault Set Management**: Redis SET with pub/sub notifications

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
ctx := context.Background()
ipc.PublishTyped(client, ctx, "vehicle:events", VehicleState{State: "ready", Speed: 0})
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
ipc.SendRequest(client, ctx, "scooter:commands", Command{Action: "unlock"})
```

## LibreScoot Hash Pattern

The LibreScoot pattern stores state in Redis hashes and notifies via pub/sub:
- Publisher: `HSET vehicle state "ready"` → `PUBLISH vehicle "state"`
- Consumer: `SUBSCRIBE vehicle` → receives `"state"` → `HGET vehicle state`

### HashPublisher

```go
// Create publisher for "vehicle" hash (publishes to "vehicle" channel)
vehicle := client.NewHashPublisher("vehicle")

// Set field and publish atomically
vehicle.Set(ctx, "state", "ready")

// Only publish if value changed
changed, _ := vehicle.SetIfChanged(ctx, "state", "ready")

// Batch updates with selective publishing
vehicle.SetManyIfChanged(ctx, map[string]any{
    "state":      "parked",
    "kickstand":  "down",
    "brake:left": "off",
})

// Set with automatic timestamp field
vehicle.SetWithTimestamp(ctx, "state", "ready")
// Sets both "state" and "state:timestamp"
```

### HashWatcher

```go
// Create watcher for "battery:0" hash
watcher := client.NewHashWatcher("battery:0")

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

// Start watching
watcher.Start()
defer watcher.Stop()

// Fetch initial state
all, _ := watcher.FetchAll(ctx)
```

### FaultSet

```go
// Manage fault codes in a Redis SET with pub/sub notification
faults := client.NewFaultSet("battery:0:fault", "battery:0", "fault")

faults.Add(ctx, 35)     // SADD + PUBLISH
faults.Remove(ctx, 35)  // SREM + PUBLISH
faults.Has(ctx, 35)     // SISMEMBER
faults.All(ctx)         // SMEMBERS
```

## Transactions

```go
tx := client.NewTxGroup()
tx.Add("HSET", "vehicle", "state", "ready").
   Add("HSET", "vehicle", "state:timestamp", time.Now().Unix()).
   Add("PUBLISH", "vehicle", "state")

results, err := tx.Exec(ctx)
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
ipc.PublishRouted(client, ctx, "events", "state", StateMsg{...})
```

## Configuration Options

```go
client, err := ipc.New(
    ipc.WithAddress("localhost"),
    ipc.WithPort(6379),
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

All operations accept `context.Context`:

```go
// Strings
client.Get(ctx, "key")
client.Set(ctx, "key", "value", 0)
client.Incr(ctx, "counter")

// Hashes
client.HGet(ctx, "hash", "field")
client.HSet(ctx, "hash", "field", "value")
client.HGetAll(ctx, "hash")

// Lists
client.LPush(ctx, "queue", "value")
client.BRPop(ctx, time.Second, "queue")

// Pub/Sub
client.Publish(ctx, "channel", "message")

// Keys
client.Exists(ctx, "key")
client.Del(ctx, "key")
client.Expire(ctx, "key", time.Hour)

// Raw command
client.Do(ctx, "PING")

// Access underlying go-redis client
client.Raw().Scan(ctx, ...)
```

## License

AGPL-3.0, see LICENSE for details.
