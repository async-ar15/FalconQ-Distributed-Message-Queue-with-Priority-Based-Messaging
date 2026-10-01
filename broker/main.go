// broker/main.go
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/dgraph-io/badger/v4"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/stathat/consistent"
)

const (
	DefaultNumPartitionsConstant = 3                   // Default partitions per topic if not specified
	MaxNumPartitions             = 256                 // Safety limit for number of partitions
	raftApplyTimeout             = 10 * time.Second    // Timeout for Raft Apply operations
	raftDataDirBase              = "falconq_raft_data" // Base directory for Raft logs (per node)
)

var broker *Broker // Global broker instance, initialized in main

// --- API Error Structure ---
type ApiError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

func NewApiError(errType, message, details string) ApiError {
	return ApiError{Type: errType, Message: message, Details: details}
}

// --- Raft Command Structure ---
type RaftCommand struct {
	Operation string `json:"op"`        // e.g., "append"
	Topic     string `json:"topic"`     // Topic name
	Partition int    `json:"partition"` // Partition ID
	Offset    uint64 `json:"offset"`    // Offset assigned by the LEADER
	Value     string `json:"value"`     // Message content
	Priority  string `json:"priority"`  // Message priority
}

// --- Core Data Structures ---
type Message struct {
	Value        string  `json:"message"`
	Priority     string  `json:"priority"`
	PartitionKey *string `json:"partitionKey,omitempty"`
}
type StoredMessage struct {
	Value    string `json:"v"`
	Priority string `json:"p"`
	Offset   uint64 `json:"-"`
} // Offset for API, not stored in Badger value

type Partition struct {
	ID        int
	TopicName string
	db        *badger.DB       // For message data (state machine)
	offsetSeq *badger.Sequence // Message offset generator (ONLY USED BY LEADER)
	raftNode  *raft.Raft       // Raft instance for this partition
	raftFSM   *partitionFSM    // FSM for this partition
	// TODO: Add retention policy logic later
}

type Topic struct {
	Name              string
	Partitions        map[int]*Partition
	mu                sync.RWMutex // Protects Partitions map & roundRobinCounter
	db                *badger.DB
	Config            *Config                // Reference to global config for peer info
	NodeID            string                 // Current node's ID
	RaftTransport     *raft.NetworkTransport // Shared Raft transport for this node
	NumPartitions     int
	HashRing          *consistent.Consistent
	roundRobinCounter uint64
}

type Broker struct {
	Topics        map[string]*Topic
	mu            sync.RWMutex
	db            *badger.DB
	Config        *Config                // Store global config here
	NodeID        string                 // Current node's ID
	RaftTransport *raft.NetworkTransport // Shared Raft transport for this node
}

// --- Raft FSM (Finite State Machine) ---
type partitionFSM struct {
	db          *badger.DB // BadgerDB for this partition's messages
	topicName   string
	partitionID int
}

func (fsm *partitionFSM) Apply(rlog *raft.Log) interface{} {
	var cmd RaftCommand
	if err := json.Unmarshal(rlog.Data, &cmd); err != nil {
		log.Printf("FATAL: FSM Apply: Unmarshal Raft log for %s p%d: %v. Data: %s", fsm.topicName, fsm.partitionID, err, string(rlog.Data))
		return fmt.Errorf("unmarshal RaftCommand: %w", err) // Return error for Raft to handle
	}
	if cmd.Operation != "append" {
		log.Printf("WARN: FSM Apply: Unknown op '%s' for %s p%d", cmd.Operation, fsm.topicName, fsm.partitionID)
		return fmt.Errorf("unknown op: %s", cmd.Operation)
	}
	key := GenerateMessageKey(cmd.Topic, cmd.Partition, cmd.Offset)
	storedMsgData := StoredMessage{Value: cmd.Value, Priority: cmd.Priority} // cmd.Offset not stored in value
	valueBytes, err := json.Marshal(storedMsgData)
	if err != nil {
		log.Printf("FATAL: FSM Apply: Marshal StoredMessage for %s p%d offset %d: %v", cmd.Topic, cmd.Partition, cmd.Offset, err)
		return fmt.Errorf("marshal StoredMessage: %w", err)
	}
	err = fsm.db.Update(func(txn *badger.Txn) error { return txn.Set(key, valueBytes) })
	if err != nil {
		log.Printf("FATAL: FSM Apply: Set key %s in BadgerDB for %s p%d: %v", key, cmd.Topic, cmd.Partition, err)
		return fmt.Errorf("FSM apply Set: %w", err)
	}
	return nil
}
func (fsm *partitionFSM) Snapshot() (raft.FSMSnapshot, error) { return &dummySnapshot{}, nil }
func (fsm *partitionFSM) Restore(rc io.ReadCloser) error {
	log.Printf("WARN: FSM Restore called for %s p%d (not implemented).", fsm.topicName, fsm.partitionID)
	return rc.Close()
}

type dummySnapshot struct{}

func (s *dummySnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (s *dummySnapshot) Release()                             {}

// --- Initialization ---
func NewBroker(db *badger.DB, cfg *Config, transport *raft.NetworkTransport) *Broker {
	return &Broker{Topics: make(map[string]*Topic), db: db, Config: cfg, NodeID: cfg.CurrentNodeID, RaftTransport: transport}
}
func NewTopic(name string, db *badger.DB, numPartitions int, cfg *Config, nodeID string, transport *raft.NetworkTransport) (*Topic, error) {
	if numPartitions <= 0 {
		numPartitions = cfg.DefaultNumPartitions
	}
	if numPartitions > MaxNumPartitions {
		return nil, fmt.Errorf("partitions %d > max %d", numPartitions, MaxNumPartitions)
	}
	members := make([]string, numPartitions)
	for i := 0; i < numPartitions; i++ {
		members[i] = strconv.Itoa(i)
	}
	hashRing := consistent.New()
	hashRing.NumberOfReplicas = 20
	hashRing.Set(members)
	return &Topic{Name: name, Partitions: make(map[int]*Partition), mu: sync.RWMutex{}, db: db, Config: cfg, NodeID: nodeID, RaftTransport: transport, NumPartitions: numPartitions, HashRing: hashRing}, nil
}
func InitializePartition(id int, topicName string, db *badger.DB, cfg *Config, transport *raft.NetworkTransport) (*Partition, error) {
	msgSeqKey := []byte(fmt.Sprintf("t/%s/p/%d/_msg_seq", topicName, id))
	msgSeq, err := db.GetSequence(msgSeqKey, 100)
	if err != nil {
		return nil, fmt.Errorf("get msg sequence for %s p%d: %w", topicName, id, err)
	}
	p := &Partition{ID: id, TopicName: topicName, db: db, offsetSeq: msgSeq}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(fmt.Sprintf("%s:%s_p%d", cfg.CurrentNodeID, topicName, id))
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("raft-%s-P%d-%s", topicName, id, cfg.CurrentNodeID),
		Output: io.Discard, // Use os.Stdout for debugging Raft core logs
		Level:  hclog.Error,
	})

	partitionRaftDir := filepath.Join(raftDataDirBase, cfg.CurrentNodeID, topicName, fmt.Sprintf("p%d", id))
	if err := os.MkdirAll(partitionRaftDir, 0700); err != nil {
		return nil, fmt.Errorf("create Raft dir %s: %w", partitionRaftDir, err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(partitionRaftDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("NewBoltStore(log) %s p%d: %w", topicName, id, err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(partitionRaftDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("NewBoltStore(stable) %s p%d: %w", topicName, id, err)
	}
	snapshotStore := raft.NewInmemSnapshotStore()
	fsm := &partitionFSM{db: db, topicName: topicName, partitionID: id}
	p.raftFSM = fsm
	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("NewRaft for %s p%d: %w", topicName, id, err)
	}
	p.raftNode = raftNode

	var raftServers []raft.Server
	for peerNodeID, peerNodeConfig := range cfg.Nodes {
		peerRaftID := raft.ServerID(fmt.Sprintf("%s:%s_p%d", peerNodeID, topicName, id))
		raftServers = append(raftServers, raft.Server{ID: peerRaftID, Address: raft.ServerAddress(peerNodeConfig.RaftAddress), Suffrage: raft.Voter})
	}
	if len(raftServers) == 0 {
		log.Printf("WARN: No peers in config for Raft group %s p%d. Standalone mode.", topicName, id)
		configuration := raft.Configuration{Servers: []raft.Server{{ID: raftConfig.LocalID, Address: transport.LocalAddr(), Suffrage: raft.Voter}}}
		sf := p.raftNode.BootstrapCluster(configuration)
		if err := sf.Error(); err != nil {
			log.Printf("WARN: Bootstrap single-node Raft %s p%d: %v", topicName, id, err)
		}
	} else {
		hasState, _ := raft.HasExistingState(logStore, stableStore, snapshotStore)
		var sortedNodeIDs []string
		for nodeID := range cfg.Nodes {
			sortedNodeIDs = append(sortedNodeIDs, nodeID)
		}
		sort.Strings(sortedNodeIDs)
		isDesignatedBootstrapper := len(sortedNodeIDs) > 0 && sortedNodeIDs[0] == cfg.CurrentNodeID
		if !hasState && isDesignatedBootstrapper {
			log.Printf("Node %s bootstrapping Raft for %s p%d with servers: %+v", cfg.CurrentNodeID, topicName, id, raftServers)
			configuration := raft.Configuration{Servers: raftServers}
			f := p.raftNode.BootstrapCluster(configuration)
			if err := f.Error(); err != nil {
				log.Printf("WARN: Bootstrap Raft for %s p%d: %v", topicName, id, err)
			}
		} else if hasState {
			log.Printf("Found existing Raft state for %s p%d on %s", topicName, id, cfg.CurrentNodeID)
		} else {
			log.Printf("Node %s for %s p%d not bootstrapper; awaiting join/leadership.", cfg.CurrentNodeID, topicName, id)
		}
	}
	return p, nil
}

// --- Broker/Topic Methods ---
func (b *Broker) getOrCreateTopic(topicName string) (*Topic, error) {
	b.mu.RLock()
	topic, exists := b.Topics[topicName]
	b.mu.RUnlock()
	if exists {
		return topic, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	topic, exists = b.Topics[topicName]
	if exists {
		return topic, nil
	}
	var err error
	topic, err = NewTopic(topicName, b.db, b.Config.DefaultNumPartitions, b.Config, b.NodeID, b.RaftTransport)
	if err != nil {
		return nil, fmt.Errorf("create topic %s: %w", topicName, err)
	}
	log.Printf("✨ Topic '%s' created with %d partitions.", topicName, topic.NumPartitions)
	b.Topics[topicName] = topic
	for i := 0; i < topic.NumPartitions; i++ {
		if _, errInit := topic.getOrCreatePartition(i); errInit != nil {
			log.Printf("ERROR: Eager init partition %d for %s: %v", i, topicName, errInit)
		}
	}
	return topic, nil
}
func (t *Topic) getOrCreatePartition(partitionID int) (*Partition, error) {
	if partitionID < 0 || partitionID >= t.NumPartitions {
		return nil, fmt.Errorf("invalid partition ID %d for %s (max %d)", partitionID, t.Name, t.NumPartitions-1)
	}
	t.mu.RLock()
	partition, exists := t.Partitions[partitionID]
	t.mu.RUnlock()
	if exists {
		return partition, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	partition, exists = t.Partitions[partitionID]
	if exists {
		return partition, nil
	}
	var err error
	partition, err = InitializePartition(partitionID, t.Name, t.db, t.Config, t.RaftTransport)
	if err != nil {
		log.Printf("❌ Failed init partition %d for %s: %v", partitionID, t.Name, err)
		return nil, err
	}
	log.Printf("🔧 Partition %d for topic '%s' initialized with Raft.", partitionID, t.Name)
	t.Partitions[partitionID] = partition
	return partition, nil
}
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	log.Printf("Releasing resources for %d topics...", len(b.Topics))
	topicsToClose := make([]*Topic, 0, len(b.Topics))
	for _, topic := range b.Topics {
		topicsToClose = append(topicsToClose, topic)
	}
	for _, topic := range topicsToClose {
		topic.mu.Lock()
		partitionsToClose := make([]*Partition, 0, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			partitionsToClose = append(partitionsToClose, partition)
		}
		topic.mu.Unlock()
		for _, partition := range partitionsToClose {
			if partition.offsetSeq != nil {
				if err := partition.offsetSeq.Release(); err != nil {
					log.Printf("WARN: Release msg sequence %s p%d: %v", topic.Name, partition.ID, err)
				}
			}
			if partition.raftNode != nil {
				sf := partition.raftNode.Shutdown()
				if err := sf.Error(); err != nil {
					log.Printf("WARN: Shutdown Raft %s p%d: %v", topic.Name, partition.ID, err)
				}
			}
		}
	}
	log.Println("Closing BadgerDB...")
	return b.db.Close()
}

// --- Partition Methods (Key Generators, Persistence, Offset Management) ---
