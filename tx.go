package redis_ipc

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Tx groups operations from multiple redis-ipc types into one atomic pipeline (MULTI/EXEC).
// Cache updates are deferred and only applied after a successful Exec.
type Tx struct {
	client  *Client
	ctx     context.Context
	pipe    redis.Pipeliner
	commits []func()
}

// NewTx creates a new transaction using the client's context.
func (c *Client) NewTx() *Tx {
	return &Tx{
		client: c,
		ctx:    c.Context(),
		pipe:   c.redis.TxPipeline(),
	}
}

// NewTxWithContext creates a new transaction with a specific context.
func (c *Client) NewTxWithContext(ctx context.Context) *Tx {
	return &Tx{
		client: c,
		ctx:    ctx,
		pipe:   c.redis.TxPipeline(),
	}
}

// HashSetManyIfChanged queues HSET for all fields and PUBLISH for each changed field.
// Returns the list of fields that differ from the cached state.
// The cache is only updated if Exec succeeds.
func (tx *Tx) HashSetManyIfChanged(hp *HashPublisher, fields map[string]any) []string {
	commit, changed := hp.queueSetManyIfChanged(tx.ctx, tx.pipe, fields)
	if commit != nil {
		tx.commits = append(tx.commits, commit)
	}
	return changed
}

// FaultAdd queues SADD and PUBLISH for a fault code.
func (tx *Tx) FaultAdd(fs *FaultSet, code int) {
	fs.queueAdd(tx.ctx, tx.pipe, code)
}

// FaultRemove queues SREM and PUBLISH for a fault code.
func (tx *Tx) FaultRemove(fs *FaultSet, code int) {
	fs.queueRemove(tx.ctx, tx.pipe, code)
}

// FaultUpdate queues a batch of adds and removes with a single PUBLISH.
func (tx *Tx) FaultUpdate(fs *FaultSet, adds, removes []int) {
	fs.queueUpdate(tx.ctx, tx.pipe, adds, removes)
}

// StreamAdd queues XADD for a stream message.
func (tx *Tx) StreamAdd(sp *StreamPublisher, values map[string]any) {
	sp.queueAdd(tx.ctx, tx.pipe, values)
}

// Exec executes all queued operations atomically via MULTI/EXEC.
// On success, applies deferred cache updates. On failure, the cache is unchanged.
func (tx *Tx) Exec() error {
	if _, err := tx.pipe.Exec(tx.ctx); err != nil {
		return err
	}
	for _, commit := range tx.commits {
		commit()
	}
	return nil
}
