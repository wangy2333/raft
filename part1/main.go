package main

import "raft/part1/client"

func main() {
	c := client.NewClient()
	c.Start()
}
