// Package main is the main package for the application
//
// Forked from brave/go-sync at commit d564bc8ccc71b4e60c9e29bbfd12a32624adbbbf,
// replacing the DynamoDB datastore and Redis cache with an embedded bbolt
// datastore (datastore/bolt.go) and an in-process go-cache-backed cache
// (cache/memcache.go). All protocol/auth/protobuf handling is untouched.
package main

import (
	"github.com/brave/go-sync/server"
)

func main() {
	server.StartServer()
}
