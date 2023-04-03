package delay

import (
	"container/list"
	"sync"
	"time"
)

type task struct {
	key      string
	expireAt time.Time
	job      func()
}
type taskLocation struct {
	elem *list.Element
	slot uint32
}
type bucket struct {
	bucketLock sync.Mutex
	list       *list.List
}

type Wheel struct {
	currentDuration time.Duration
	current         uint32
	interval        time.Duration
	ticker          *time.Ticker
	slots           []*bucket
	slotsNum        uint32
}

type TimeWheel struct {
	mswheel       *Wheel
	swheel        *Wheel
	mwheel        *Wheel
	hwheel        *Wheel
	addC          chan *task
	removeC       chan string
	stopC         chan struct{}
	taskLocations map[string]*taskLocation
}

func (tw *TimeWheel) Start() {
	go func() {
		for {
			select {
			case <-tw.mswheel.ticker.C:
				tw.tick()
			case t := <-tw.addC:
				tw.add(t)
			case key := <-tw.removeC:
				tw.remove(key)
			case <-tw.stopC:
				tw.stop()
				return
			}
		}
	}()
}

func (tw *TimeWheel) Add(key string, expireAt time.Time, job func()) {
	tw.addC <- &task{
		key:      key,
		expireAt: expireAt,
		job:      job,
	}
}

func (tw *TimeWheel) add(t *task) {
	at := t.expireAt
	milli := at.UnixMilli()
	now := time.Now().UnixMilli()

	if milli-now < int64(tw.mswheel.interval) {
		tw.addTaskToWheel(t, tw.mswheel)
	} else if milli-now < int64(tw.swheel.interval)*int64(tw.swheel.slotsNum) {
		tw.addTaskToWheel(t, tw.swheel)
	} else if milli-now < int64(tw.mwheel.interval)*int64(tw.mwheel.slotsNum) {
		tw.addTaskToWheel(t, tw.mwheel)
	} else if milli-now < int64(tw.hwheel.interval)*int64(tw.hwheel.slotsNum) {
		tw.addTaskToWheel(t, tw.hwheel)
	} else {
		// If the task expires beyond the maximum interval supported by the time wheel, ignore the task.
		return
	}
}

func (tw *TimeWheel) addTaskToWheel(t *task, wheel *Wheel) {
	pos := (wheel.current + uint32((t.expireAt.UnixMilli()-time.Now().UnixMilli())/int64(wheel.interval))) % wheel.slotsNum
	b := wheel.slots[pos]
	b.bucketLock.Lock()
	back := b.list.PushBack(t)
	b.bucketLock.Unlock()
	tw.taskLocations[t.key] = &taskLocation{
		elem: back,
		slot: pos,
	}
}

func (tw *TimeWheel) Remove(key string) {
	tw.removeC <- key
}

func (tw *TimeWheel) remove(key string) {
	loc, ok := tw.taskLocations[key]
	if !ok {
		return
	}
	delete(tw.taskLocations, key)
	b := tw.mswheel.slots[loc.slot]
	b.bucketLock.Lock()
	b.list.Remove(loc.elem)
	b.bucketLock.Unlock()
}

func (tw *TimeWheel) tick() {
	tw.mswheel.tick()
	if tw.mswheel.current == 0 {
		tw.swheel.tick()
		if tw.swheel.current == 0 {
			tw.mwheel.tick()
			if tw.mwheel.current == 0 {
				tw.hwheel.tick()
			}
		}
	}
}

func (tw *TimeWheel) stop() {
	tw.mswheel.ticker.Stop()
	close(tw.addC)
	close(tw.removeC)
	close(tw.stopC)
	tw.taskLocations = nil

}

func (w *Wheel) tick() {
	w.current = (w.current + 1) % w.slotsNum
	b := w.slots[w.current]
	b.bucketLock.Lock()
	for e := b.list.Front(); e != nil; {
		next := e.Next()
		task := e.Value.(*task)
		if task.expireAt.UnixMilli() <= time.Now().UnixMilli() {
			go task.job()
			b.list.Remove(e)
		}
		e = next
	}
	b.bucketLock.Unlock()
}

func NewWheel(interval time.Duration, slotsNum uint32) *Wheel {
	w := &Wheel{
		interval:        interval,
		ticker:          time.NewTicker(interval),
		slots:           make([]*bucket, slotsNum),
		slotsNum:        slotsNum,
		currentDuration: 0,
		current:         0,
	}
	for i := range w.slots {
		w.slots[i] = &bucket{
			list: list.New(),
		}
	}
	return w
}
func NewTimeWheel() *TimeWheel {
	return &TimeWheel{
		mswheel:       NewWheel(10*time.Millisecond, 100),
		swheel:        NewWheel(time.Second, 60),
		mwheel:        NewWheel(time.Minute, 60),
		hwheel:        NewWheel(time.Hour, 24),
		addC:          make(chan *task),
		removeC:       make(chan string),
		stopC:         make(chan struct{}),
		taskLocations: map[string]*taskLocation{},
	}

}
