package cluster

import (
	"errors"
	"fmt"
	"math/rand"
	cm "mygodis/common"
	cmi "mygodis/common/commoninterface"
	"mygodis/config"
	"mygodis/resp"
	"mygodis/util/cmdutil"
	"mygodis/util/com"
	"strconv"
	"time"
)

const Fanout = 1 << 2

func (c *Cluster) broadcast(args cm.CmdLine) (result map[string]resp.Reply, errs []error) {
	result = make(map[string]resp.Reply)
	nodes := c.nodes.Keys()
	for _, node := range nodes {
		if node == c.self {
			continue
		}
		client := c.nodeConnectionPool.GetConnection(node)
		reply, _ := client.Send(args)
		if resp.IsErrorReply(reply) {
			errs = append(errs, errors.New("broadcast error"+string(reply.ToBytes())))
		}
		result[node] = reply
	}
	return result, errs
}
func (c *Cluster) isAllOk(spreadResult map[string]resp.Reply) bool {
	for _, reply := range spreadResult {
		isErrorReply := resp.IsErrorReply(reply)
		if isErrorReply {
			return false
		}
	}
	return true
}
func (c *Cluster) isMoreThanHalfOk(spreadResult map[string]resp.Reply) bool {
	length := len(spreadResult)
	okCount := 0
	for _, reply := range spreadResult {
		isErrorReply := resp.IsErrorReply(reply)
		if !isErrorReply {
			okCount++
		}
	}
	return okCount > length/2
}
func (c *Cluster) toClusterCommand(newTTL int, args ...string) cm.CmdLine {
	cmd := make(cm.CmdLine, 0)
	cmd = append(cmd, []byte("CLUSTER"))
	c.epoch++
	epoch := c.epoch
	cmd = append(cmd, []byte(fmt.Sprintf("%d", epoch)))
	cmd = append(cmd, []byte(c.self))
	cmd = append(cmd, []byte(strconv.Itoa(newTTL)))
	for _, arg := range args {
		cmd = append(cmd, []byte(arg))
	}
	return cmd
}
func parseClusterCommand(line cm.CmdLine) (epoch uint64, originate string, TTL int, args cm.CmdLine, err error) {
	epoch, err = strconv.ParseUint(string(line[1]), 10, 64)
	if err != nil {
		return
	}
	originate = string(line[2])
	TTL, err = strconv.Atoi(string(line[3]))
	if err != nil {
		return
	}
	args = line[4:]
	return epoch, originate, TTL, args, nil
}
func isAuthenticated(c cmi.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}
func pickNodes(nodes []string, probability float64, fanout int, excludesNode ...string) []string {
	if probability < 0 || probability > 1 {
		panic("Probability must be between 0 and 1")
	}
	if len(nodes) == 0 {
		return []string{}
	}
	filteredNodes := make([]string, 0)
	excludeMap := make(map[string]bool)
	for _, node := range excludesNode {
		excludeMap[node] = true
	}
	for _, node := range nodes {
		if !excludeMap[node] {
			filteredNodes = append(filteredNodes, node)
		}
	}
	if len(filteredNodes) == 0 {
		return []string{}
	}
	rand.Seed(time.Now().UnixNano())
	selectedNodes := make([]string, 0)
	for _, node := range filteredNodes {
		if rand.Float64() <= probability {
			selectedNodes = append(selectedNodes, node)
		}
		if len(selectedNodes) >= fanout {
			break
		}
	}
	if len(selectedNodes) == 0 {
		index := rand.Intn(len(filteredNodes))
		selectedNodes = append(selectedNodes, filteredNodes[index])
	}
	return selectedNodes
}
func (c *Cluster) spread(args cm.CmdLine) map[string]resp.Reply {
	result := make(map[string]resp.Reply)
	nodes := c.nodes.Keys()
	pickedNodes := pickNodes(nodes, 0.5, Fanout, c.self)
	for _, node := range pickedNodes {
		client := c.nodeConnectionPool.GetConnection(node)
		reply, _ := client.Send(args)
		result[node] = reply
	}
	return result
}
func (c *Cluster) addNewNode(node string) []byte {
	c.nodes.Put(node, struct{}{})
	c.nodeConnectionPool.AddConnection(node)
	c.ch.AddNode(node)
	serialize, err := c.ch.Serialize()
	if err != nil {
		panic(err)
	}
	return serialize
}
func (c *Cluster) execCluster(connection cmi.Connection, args cm.CmdLine) (reply resp.Reply) {
	clusterCommand := string(args[0])
	switch clusterCommand {
	case "MEET":
		reply = c.execMeet(args[1:])
	case "JOIN":
		reply = c.execJoin(connection, args[1:])
	case "ADDNODE":
		reply = c.execAddNode(args[1:])
	case "CFLUSHDB":
		reply = c.execCFlushDB()
	case "NODES":
		reply = c.execNodes()
		//case "CNODES":
		//	reply = c.execCNodes()
	}
	//c.dumpCluster()
	return
}
func (c *Cluster) execNodes() resp.Reply {
	nodes := c.nodes.Keys()
	//results, errs := c.broadcast(cmdutil.ToCmdLineWithName("CLUSTER", "CNODES"))
	//if errs != nil || len(errs) != 0 {
	//	return resp.MakeErrReply("cluster error")
	//}
	//resultNodes := make([][]string, len(nodes))
	//for _, reply := range results {
	//	itemNodes, ok := reply.(*resp.MultiBulkReply)
	//	if ok {
	//		resultNodes = append(resultNodes, com.BytesToString(itemNodes.Args))
	//	}
	//}
	//resultNodes = append(resultNodes, nodes)
	//if com.AreSlicesEqual(resultNodes...) {
	return resp.MakeMultiBulkReply(com.StringsToBytes(nodes))
	//}
	//return resp.MakeErrReply("cluster error")
}
func (c *Cluster) execCNodes() resp.Reply {
	nodes := c.nodes.Keys()
	nodes = append(nodes, c.self)
	result := com.StringsToBytes(nodes)
	return resp.MakeMultiBulkReply(result)
}
func (c *Cluster) execMeet(args cm.CmdLine) resp.Reply {
	targetNode := string(args[0])
	connectionPool := c.nodeConnectionPool
	client := connectionPool.GetConnection(targetNode)
	chbytes, _ := client.Send(cmdutil.ToCmdLineWithName("CLUSTER", "JOIN", c.self))
	bulkReply, ok := chbytes.(*resp.SimpleStringReply)
	if ok {
		arg := bulkReply.SimpleString
		ch, err := LoadFrom([]byte(arg))
		if err != nil {
			return resp.MakeErrReply(err.Error())
		}
		clusterNodes := ch.GetNodes()
		c.nodeConnectionPool.AddConnection(clusterNodes...)
		c.ch = ch
		c.nodes.Put(targetNode, struct{}{})
		return resp.MakeOkReply()
	}
	return resp.MakeErrReply("meet failed")
}
func (c *Cluster) execAddNode(args cm.CmdLine) resp.Reply {
	targetNode := string(args[0])
	c.nodes.Put(targetNode, struct{}{})
	c.ch.AddNode(targetNode)
	return resp.MakeOkReply()
}
func (c *Cluster) execJoin(connection cmi.Connection, line cm.CmdLine) resp.Reply {
	newNode := string(line[0])
	chbytes := c.addNewNode(newNode)
	broadcastResult, errs := c.broadcast(cmdutil.ToCmdLineWithName("CLUSTER", "ADDNODE", newNode))
	if len(errs) == 0 && c.isAllOk(broadcastResult) {
		return resp.MakeSimpleStringReply(string(chbytes))
	}
	return resp.MakeErrReply("join failed")
}
func (c *Cluster) execCFlushDB() resp.Reply {
	return c.db.FlushAll()
}
