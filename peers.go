/*
Copyright 2012 Google Inc.

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

// peers.go defines how processes find and communicate with their peers.

package groupcache

import (
	"context"

	pb "github.com/bjornleffler/groupcache/groupcachepb"
	emptypb "github.com/golang/protobuf/ptypes/empty"
)

// Context is an alias to context.Context for backwards compatibility purposes.
type Context = context.Context

// Peer is the interface that must be implemented by a peer.
type Peer interface {
	Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error
	Set(ctx context.Context, in *pb.SetRequest, out *emptypb.Empty) error
	Delete(ctx context.Context, in *pb.DeleteRequest, out *emptypb.Empty) error
}

// PeerPicker is the interface that must be implemented to locate
// the peer that owns a specific key.
type PeerPicker interface {
	// PickPeer returns the peer that owns the specific key
	// and true to indicate that a remote peer was nominated.
	// It returns nil, false if the key owner is the current peer.
	PickPeer(key string) (peer Peer, ok bool)
}

// NoPeers is an implementation of PeerPicker that never finds a peer.
type NoPeers struct{}

func (NoPeers) PickPeer(key string) (peer Peer, ok bool) { return }

var (
	portPicker func(groupName string) PeerPicker
)

// RegisterPeerPicker registers the peer initialization function.
// It is called once, when the first group is created.
// Either RegisterPeerPicker or RegisterPerGroupPeerPicker should be
// called exactly once, but not both.
func RegisterPeerPicker(fn func() PeerPicker) {
	if portPicker != nil {
		panic("RegisterPeerPicker called more than once")
	}
	portPicker = func(_ string) PeerPicker { return fn() }
}

// UnRegisterPeerPicker allows unit tests to call RegisterPeerPicker() multiple times.
func UnRegisterPeerPicker() {
	portPicker = nil
}

// RegisterPerGroupPeerPicker registers the peer initialization function,
// which takes the groupName, to be used in choosing a PeerPicker.
// It is called once, when the first group is created.
// Either RegisterPeerPicker or RegisterPerGroupPeerPicker should be
// called exactly once, but not both.
func RegisterPerGroupPeerPicker(fn func(groupName string) PeerPicker) {
	if portPicker != nil {
		panic("RegisterPerGroupPeerPicker called more than once")
	}
	portPicker = fn
}

func getPeers(groupName string) PeerPicker {
	if portPicker == nil {
		return NoPeers{}
	}
	pk := portPicker(groupName)
	if pk == nil {
		pk = NoPeers{}
	}
	return pk
}
