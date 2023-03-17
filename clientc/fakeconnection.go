package clientc

import cm "mygodis/common"

type FakeConnection struct {
	DBindex int
}

func NewFakeConnection() *FakeConnection {
	return &FakeConnection{}
}
func (f *FakeConnection) Write(bytes []byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) Close() error {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) SetPassword(s string) {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) GetPassword() string {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) Subscribe(channel string) {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) UnSubscribe(channel string) {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) SubsCount() int {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) GetChannels() []string {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) InMultiState() bool {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) SetMultiState(b bool) {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) GetQueuedCmdLine() []cm.CmdLine {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) EnqueueCmd(i [][]byte) {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) ClearQueuedCmds() {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) GetWatching() map[string]uint32 {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) AddTxError(err error) {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) GetTxErrors() []error {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) GetDBIndex() int {
	return f.DBindex
}

func (f *FakeConnection) SelectDB(i int) {
	f.DBindex = i
}

func (f *FakeConnection) SetSlave() {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) IsSlave() bool {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) SetMaster() {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) IsMaster() bool {
	//TODO implement me
	panic("implement me")
}

func (f *FakeConnection) Name() string {
	//TODO implement me
	panic("implement me")
}
