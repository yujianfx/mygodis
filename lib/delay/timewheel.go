package delay

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type task struct {
	key          string
	expireAt     time.Time
	job          func()
	currentLevel int
}

type taskLocation struct {
	elem *list.Element
	slot int64
}

type bucket struct {
	bucketLock sync.Mutex
	list       *list.List
}

type Wheel struct {
	currentDuration time.Duration
	current         int64
	interval        time.Duration
	maxDuration     time.Duration
	slots           []*bucket
	slotsNum        int64
	name            string
}

type TimeWheel struct {
	wheels        []*Wheel
	addC          chan *task
	ticker        *time.Ticker
	removeC       chan string
	stopC         chan struct{}
	taskLocations map[string]*taskLocation
}

func doJob(t *task) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("任务执行失败", err)
		}
	}()
	t.job()
}

func (tw *TimeWheel) add(t *task) {
	milliseconds := t.expireAt.Sub(time.Now()).Milliseconds()
	for _, w := range tw.wheels {
		if milliseconds > w.maxDuration.Milliseconds() {
			t.currentLevel++
		} else {
			break
		}
	}
	tw.addTaskToWheel(t, tw.wheels[t.currentLevel])
}

func (tw *TimeWheel) addTaskToWheel(t *task, wheel *Wheel) {
	slot := (t.expireAt.UnixMilli() - time.Now().UnixMilli()) / wheel.interval.Milliseconds()
	position := (wheel.current + slot) % wheel.slotsNum
	wheel.slots[position].bucketLock.Lock()
	elem := wheel.slots[position].list.PushBack(t)
	wheel.slots[position].bucketLock.Unlock()
	tw.taskLocations[t.key] = &taskLocation{
		elem: elem,
		slot: position,
	}
}

func (tw *TimeWheel) handleTick() {
	for _, w := range tw.wheels {
		w.current = (w.current + 1) % w.slotsNum
		bucket := w.slots[w.current]
		if bucket.list.Len() == 0 {
			continue
		}
		front := bucket.list.Front()
		if front != nil {
			tk := front.Value.(*task)
			if tk.currentLevel == 0 || tk.expireAt.UnixMilli() <= time.Now().UnixMilli() {
				for front != nil {
					t := front.Value.(*task)
					if t.expireAt.UnixMilli() <= time.Now().UnixMilli() {
						doJob(t)
						next := front.Next()
						bucket.list.Remove(front)
						front = next
						delete(tw.taskLocations, t.key)
					} else {
						break
					}
				}
			} else {
				nextWheel := tw.wheels[tk.currentLevel-1]
				nextSlot := (tk.expireAt.UnixMilli() - time.Now().UnixMilli()) / nextWheel.interval.Milliseconds()
				nextPosition := (w.current + nextSlot) % nextWheel.slotsNum
				currentSlot := tw.taskLocations[tk.key].slot
				currentBucket := w.slots[currentSlot]
				nextWheel.slots[nextPosition].list.PushBackList(currentBucket.list)
				currentBucket.list.Init()
				tw.taskLocations[tk.key].slot = nextPosition
				tk.currentLevel--
			}
		}
		if w.current == 0 {
			continue
		}
		break
	}
}

func (tw *TimeWheel) stop() {
	tw.ticker.Stop()
	close(tw.addC)
	close(tw.removeC)
	close(tw.stopC)
}

func NewWheel(interval time.Duration, slotsNum int64, name string) *Wheel {
	w := &Wheel{
		interval:        interval,
		slots:           make([]*bucket, slotsNum),
		slotsNum:        slotsNum,
		currentDuration: 0,
		current:         0,
		name:            name,
		maxDuration:     interval * time.Duration(slotsNum),
	}
	for i := range w.slots {
		w.slots[i] = &bucket{
			list: list.New(),
		}
	}
	return w
}

func NewTimeWheel() *TimeWheel {
	t := &TimeWheel{
		ticker:        time.NewTicker(1 * time.Millisecond),
		wheels:        make([]*Wheel, 4),
		addC:          make(chan *task),
		removeC:       make(chan string),
		stopC:         make(chan struct{}),
		taskLocations: map[string]*taskLocation{},
	}
	t.wheels[0] = NewWheel(10*time.Millisecond, 100, "ms")
	t.wheels[1] = NewWheel(time.Second, 60, "s")
	t.wheels[2] = NewWheel(time.Minute, 60, "m")
	t.wheels[3] = NewWheel(time.Hour, 24, "h")
	return t
}

func (tw *TimeWheel) Remove(key string) {
	tw.removeC <- key
}

func (tw *TimeWheel) Add(key string, expireAt time.Time, job func()) {
	tw.addC <- &task{
		key:      key,
		expireAt: expireAt,
		job:      job,
	}
}

func (tw *TimeWheel) Start() {
	go func() {
		for {
			select {
			case <-tw.ticker.C:
				tw.handleTick()
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

func (tw *TimeWheel) remove(key string) {
	if location, ok := tw.taskLocations[key]; ok {
		wheel := tw.wheels[location.elem.Value.(*task).currentLevel]
		wheel.slots[location.slot].bucketLock.Lock()
		wheel.slots[location.slot].list.Remove(location.elem)
		wheel.slots[location.slot].bucketLock.Unlock()
		delete(tw.taskLocations, key)
	}
}
