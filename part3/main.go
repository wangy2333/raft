package main

import (
	"raft/part3/client"
	"strconv"
	"time"
)

func main() {
	c := client.NewClient()
	c.Start()
	ticker := time.NewTicker(time.Second * 2)
	count := 1
	for {
		<-ticker.C
		c.Set("opt" + strconv.Itoa(count)) //模拟操作
		count++
	}

}
