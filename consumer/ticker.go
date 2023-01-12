package consumer

import (
	"sync"
)

type ticker struct {
	mu      sync.RWMutex
	curTick int
}

func newTicker() *ticker {
	return &ticker{}
}

func (t *ticker) setCurTick(ts int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.curTick = ts
	//fmt.Println("set ts,", ts)
}

func (t *ticker) getCurTick() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.curTick
}
