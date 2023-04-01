package delay

import (
	"container/list"
	logger "mygodis/log"
	"time"
)

type location struct {
	slotIndex int64
	taskNode  *list.Element
}
type task struct {
	delay  time.Duration
	circle int64
	key    string
	job    func()
}
type TimeWheel struct {
	interval    time.Duration
	ticker      *time.Ticker
	slots       []*list.List
	timer       map[string]*location
	current     int64
	slotNum     int64
	addTaskC    chan task
	removeTaskC chan string
	stopC       chan bool
}

func NewTimeWheel(interval time.Duration, slotNum int64) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:    interval,
		slots:       make([]*list.List, slotNum),
		timer:       make(map[string]*location),
		current:     0,
		slotNum:     slotNum,
		addTaskC:    make(chan task),
		removeTaskC: make(chan string),
		stopC:       make(chan bool),
	}
	tw.initSlots()
	return tw
}
func (tw *TimeWheel) initSlots() {
	for i := int64(0); i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}
func (tw *TimeWheel) Stop() {
	tw.stopC <- true
}
func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tick()
		case task := <-tw.addTaskC:
			tw.addTask(&task)
		case key := <-tw.removeTaskC:
			tw.removeTask(key)
		case <-tw.stopC:
			tw.ticker.Stop()
			return
		}
	}
}
func (tw *TimeWheel) tick() {
	slot := tw.slots[tw.current]
	deleteKeys := make([]string, 0)
	for e := slot.Front(); e != nil; e = e.Next() {
		t := e.Value.(*task)
		if t.circle == 0 {
			go func() {
				defer func() {
					if err := recover(); err != nil {
						logger.Error(err)
					}
				}()
				t.job()
			}()
			deleteKeys = append(deleteKeys, t.key)
		} else {
			t.circle--
		}
	}
	for _, key := range deleteKeys {
		loc, ok := tw.timer[key]
		if ok {
			slot := tw.slots[loc.slotIndex]
			slot.Remove(loc.taskNode)
			delete(tw.timer, key)
		}
	}
	tw.current = (tw.current + 1) % tw.slotNum
}
func (tw *TimeWheel) addTask(task *task) {
	i, circle := tw.getPositionAndCircle(task)
	slotIndex := i
	task.circle = circle
	slot := tw.slots[slotIndex]
	back := slot.PushBack(task)
	e := back
	tw.timer[task.key] = &location{
		slotIndex: slotIndex,
		taskNode:  e,
	}

}
func (tw *TimeWheel) removeTask(key string) {
	if loc, ok := tw.timer[key]; ok {
		slot := tw.slots[loc.slotIndex]
		slot.Remove(loc.taskNode)
		delete(tw.timer, key)
	}

}
func (tw *TimeWheel) AddTask(delay time.Duration, key string, job func()) {
	tw.addTaskC <- task{
		delay: delay,
		key:   key,
		job:   job,
	}
}
func (tw *TimeWheel) RemoveTask(key string) {
	tw.removeTaskC <- key
}
func (tw *TimeWheel) getPositionAndCircle(tk *task) (p int64, c int64) {
	circle := int64(tk.delay) / (int64(tw.interval) * tw.slotNum)
	position := (tw.current + int64(tk.delay/tw.interval)) % tw.slotNum
	return position, circle
}
