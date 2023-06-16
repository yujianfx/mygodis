# mygodis
###golang实现的redis-server 
- 支持 字符串，列表，哈希，集合，等结构
- cluster模式通过一致性哈希实现分片
- cluster模式下会自动的拆分mset mget等命令
