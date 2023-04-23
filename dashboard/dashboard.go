package dashboard

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"net/http"
	"time"
)

type Dashboard struct {
	enabled bool
	addr    string
	engine  *gin.Engine
}

var DefaultDashboard *Dashboard = &Dashboard{
	enabled: true,
	addr:    "0.0.0.0:10088",
	engine:  gin.Default(),
}

func (d *Dashboard) Start() {
	err := d.engine.Run(d.addr)
	if err != nil {
		return
	}
}
func addGetHandler(path string, h func(ctx *gin.Context)) {
	DefaultDashboard.engine.GET(path, h)
}

func init() {
	DefaultDashboard.engine.LoadHTMLFiles("dashboard.html")
	addGetHandler("/cpu", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, cpuInfo())
	})
	addGetHandler("/mem", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, memoryInfo())
	})
	addGetHandler("/", func(ctx *gin.Context) {
		ctx.HTML(http.StatusOK, "dashboard.html", gin.H{
			"title": "dashboard",
		})
	})
	addGetHandler("/api/cpu-memory", func(ctx *gin.Context) {
		cpuPercent, err := cpu.Percent(time.Second, false)
		if err != nil {
			ctx.JSON(500, gin.H{"error": fmt.Sprintf("Error getting CPU usage: %v", err)})
			return
		}
		// 获取内存使用率数据
		memInfo, err := mem.VirtualMemory()
		if err != nil {
			ctx.JSON(500, gin.H{"error": fmt.Sprintf("Error getting memory usage: %v", err)})
			return
		}
		data := gin.H{
			"timestamp":  time.Now().Format("15:04:05"),
			"cpuPercent": cpuPercent[0],
			"memPercent": memInfo.UsedPercent,
		}
		ctx.JSON(200, data)
	})
}

func cpuInfo() map[string]any {
	infoStats, err := cpu.Info()
	if err != nil {
		fmt.Println("Error getting CPU info:", err)
		return nil
	}
	cpuMap := make(map[string]any)
	for i, c := range infoStats {
		prefix := fmt.Sprintf("cpu%d_", i)
		cpuMap[prefix+"vendorID"] = c.VendorID
		cpuMap[prefix+"family"] = c.Family
		cpuMap[prefix+"model"] = c.Model
		cpuMap[prefix+"modelName"] = c.ModelName
		cpuMap[prefix+"stepping"] = c.Stepping
		cpuMap[prefix+"physicalID"] = c.PhysicalID
		cpuMap[prefix+"coreID"] = c.CoreID
		cpuMap[prefix+"cores"] = c.Cores
		cpuMap[prefix+"flags"] = c.Flags
		cpuMap[prefix+"mhz"] = c.Mhz
		cpuMap[prefix+"cacheSize"] = c.CacheSize
	}
	return cpuMap
}
func memoryInfo() map[string]any {
	vmStat, err := mem.VirtualMemory()
	if err != nil {
		fmt.Println("Error getting memory info:", err)
		return nil
	}
	memMap := make(map[string]interface{})
	memMap["total"] = vmStat.Total
	memMap["free"] = vmStat.Free
	memMap["used"] = vmStat.Used
	memMap["used_percent"] = vmStat.UsedPercent
	memMap["buffers"] = vmStat.Buffers
	memMap["cached"] = vmStat.Cached
	memMap["active"] = vmStat.Active
	memMap["inactive"] = vmStat.Inactive
	return memMap
}
