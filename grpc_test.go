/*
Copyright 2024 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Tests for groupcache grpc.

package groupcache

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/bjornleffler/groupcache/groupcachepb"
)

// TestRemoteGrpc tests that remote grpc pools work as expected.
func TestRemoteGrpc(t *testing.T) {
	UnRegisterPeerPicker()
	pool := NewRemoteGrpcPool()
	if err := pool.StartGrpcServer(); err == nil {
		t.Fatal("Expected error.")
	}

	// Test peer picking logic.
	peers := []string{"localhost:1111", "localhost:2222"}
	pool.SetPeers(peers...)

	testCases := map[string]string {
		"a": peers[1],
		"d": peers[0],
	}
	for key, expected := range testCases {
		if peer, found := pool.PickPeer(key); peer == nil {
			t.Fatalf("Unexpected nil peer. key: %q", key)
		} else if !found {
			t.Fatalf("No peer found. key: %q", key)
		} else {
			p := peer.(*grpcPeer)
			addr := fmt.Sprintf("%s:%d", p.host, p.port)
			if addr != expected {
				t.Fatalf("Expected hashing to peer %q. Got: %q", expected, addr)
			}
		}
	}
}

// TestGrpcServerGetSet tests server side Get and Set calls.
func TestGrpcServerGetSet(t *testing.T) {
	UnRegisterPeerPicker()
	groupName := t.Name() + "-group"
	key := t.Name() + "-key"
	value := t.Name() + "-value"

	// Set up a local group, different to other unit tests.
	storage := make(map[string]string)
	group := NewGroup(groupName, cacheSize, GetterFunc(func(_ context.Context, key string, dest Sink) error {
		if value, ok := storage[key]; ok {
			dest.SetString(value)
		}
		return nil
	}))
	setter := func(ctx context.Context, key string, value ByteView) error {
		storage[key] = value.String()
		return nil
	}
	group.RegisterSetter(setter)

	// Set up gRPC pool, but do NOT start the server.
	// Sending gRPC calls to self may cause deadlocks.
	port := uint(1234)
	peers := []string{"localhost:0000", "localhost:1234"}
	pool := NewGrpcPool(peers[0], port)
	pool.SetPeers(peers...)

	// 1. Value shouldn't exist.
	ctx := context.TODO()
	getReq := &pb.GetRequest{
		Group: &groupName,
		Key: &key,
	}
	if out, err := pool.Get(ctx, getReq); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else if string(out.Value) != "" {
		t.Fatalf("Expected empty value. Got %q", string(out.Value))
	}

	// 2. Set value.
	setReq := &pb.SetRequest{
		Group: &groupName,
		Key: &key,
		Value: []byte(value),
	}
	if _, err := pool.Set(ctx, setReq); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 3. Value should now exist..
	if out, err := pool.Get(ctx, getReq); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	} else if string(out.Value) != value {
		t.Fatalf("Expected %q Got %q", value, string(out.Value))
	}
}
