package cluster

import (
	"mygodis/util/cmdutil"
	"testing"
)

func TestClient_Send(t *testing.T) {
	client := MakeClient("localhost:6379")
	client.Start()
	clusterJoin := cmdutil.ToCmdLineWithName("cluster", "join", "localhost:6389")
	reply, err := client.Send(clusterJoin)
	if err != nil {
		t.Error(err)
	}
	t.Log(reply)
}
func TestClient_Send1(t *testing.T) {
	client := MakeClient("localhost:6379")
	client.Start()
	get := cmdutil.ToCmdLineWithName("GET", "k")
	reply, err := client.Send(get)
	if err != nil {
		t.Error(err)
	}
	t.Log(reply)
}
