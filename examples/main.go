package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	redis_ipc "github.com/librescoot/redis-ipc"
)

const (
	testChannel = "test-channel"
	testQueue   = "test-queue"
	testKey     = "test-key"
	testHash    = "test-hash"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	log.Printf("Initializing IPC client...")
	client, err := redis_ipc.New(
		redis_ipc.WithAddress("localhost"),
		redis_ipc.WithPort(6379),
		redis_ipc.WithRetryInterval(time.Second),
		redis_ipc.WithMaxRetries(3),
	)
	if err != nil {
		log.Fatalf("Failed to create IPC client: %v", err)
	}
	defer client.Close()

	// Subscribe to a typed channel
	log.Printf("Setting up subscription handler...")
	_, err = redis_ipc.Subscribe(client, testChannel, func(msg string) error {
		log.Printf("SUB: Received message on %s: %s", testChannel, msg)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Set up request processor
	log.Printf("Setting up request handler...")
	redis_ipc.HandleRequests(client, testQueue, func(msg string) error {
		log.Printf("QUEUE: Processing request from %s: %s", testQueue, msg)
		return nil
	})

	// Set up a hash watcher
	log.Printf("Setting up hash watcher...")
	watcher := client.NewHashWatcher(testHash)
	watcher.OnField("status", func(value string) error {
		log.Printf("HASH: status changed to %s", value)
		return nil
	})
	watcher.OnAny(func(field, value string) error {
		log.Printf("HASH: %s = %s", field, value)
		return nil
	})
	if err := watcher.Start(); err != nil {
		log.Fatalf("Failed to start hash watcher: %v", err)
	}
	defer watcher.Stop()

	// Start test publisher
	go func() {
		count := 0
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		hp := client.NewHashPublisher(testHash)

		for range ticker.C {
			count++

			// Direct key operations
			value := fmt.Sprintf("test-value-%d", count)
			log.Printf("DIRECT: Setting %s = %s", testKey, value)
			if err := client.Set(testKey, value, 0); err != nil {
				log.Printf("ERROR: SET failed: %v", err)
				continue
			}

			readValue, err := client.Get(testKey)
			if err != nil {
				log.Printf("ERROR: GET failed: %v", err)
				continue
			}
			log.Printf("DIRECT: Read %s = %s", testKey, readValue)

			// Transaction group
			txg := client.NewTxGroup()
			txg.Add("SET", testKey+"-tx", value)
			txg.Add("GET", testKey+"-tx")
			txg.Add("INCR", "counter")
			results, err := txg.Exec()
			if err != nil {
				log.Printf("ERROR: Transaction failed: %v", err)
				continue
			}
			log.Printf("TX: Results: SET=%v, GET=%v, INCR=%v",
				results[0], results[1], results[2])

			// Hash publisher (async by default)
			hp.Set("status", fmt.Sprintf("running-%d", count))
			hp.SetMany(map[string]any{
				"count":      count,
				"updated_at": time.Now().Format(time.RFC3339),
			})

			// Publish to channel
			msg := fmt.Sprintf("test-message-%d", count)
			log.Printf("PUB: Publishing to %s: %s", testChannel, msg)
			client.Publish(testChannel, msg)

			// Push to queue
			item := fmt.Sprintf("test-item-%d", count)
			log.Printf("QUEUE: Pushing to %s: %s", testQueue, item)
			if err := redis_ipc.SendRequest(client, testQueue, item); err != nil {
				log.Printf("ERROR: Queue push failed: %v", err)
			}
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("Received signal %v, shutting down...", sig)
}
