package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (k *KVStore) Get(key string) (string, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	val, ok := k.data[key]
	return val, ok
}

func (k *KVStore) Set(key, value string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data[key] = value
}

func (k *KVStore) Delete(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.data, key)
}

func (k *KVStore) GetAll() map[string]string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	result := make(map[string]string)
	for k, v := range k.data {
		result[k] = v
	}
	return result
}

// 修改 fsm 定义，使其嵌入 *KVStore
type fsm struct {
	*KVStore
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch cmd.Op {
	case "set":
		f.Set(cmd.Key, cmd.Value) // 现在可以使用嵌入的 KVStore 方法
	case "delete":
		f.Delete(cmd.Key) // 现在可以使用嵌入的 KVStore 方法
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", cmd.Op))
	}

	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.data {
		o[k] = v
	}

	return &fsmSnapshot{store: o}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.data = o
	return nil
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}

type Store struct {
	store *KVStore
	raft  *raft.Raft
}

func NewStore() *Store {
	kvStore := NewKVStore()
	return &Store{
		store: kvStore,
	}
}

func (s *Store) Open(enableSingle bool, localID string) error {
	if err := os.MkdirAll("raft", 0755); err != nil {
		return fmt.Errorf("failed to create raft directory: %s", err)
	}
	if err := os.MkdirAll("snapshots", 0755); err != nil {
		return fmt.Errorf("failed to create snapshots directory: %s", err)
	}
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", os.Args[2])
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(os.Args[2], addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore("snapshots", retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore

	boltDB, err := raftboltdb.NewBoltStore(filepath.Join("raft", localID+".db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	logStore = boltDB
	stableStore = boltDB

	// 修改这里：创建一个 fsm 实例，嵌入 store
	fsmInstance := &fsm{KVStore: s.store}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, fsmInstance, logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

func (s *Store) Get(key string) (string, bool) {
	return s.store.Get(key)
}

func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) GetAll() map[string]string {
	return s.store.GetAll()
}

func (s *Store) Join(nodeID, addr string) error {
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				return nil
			}
			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing server %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	return nil
}

// 添加CORS中间件
func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 设置CORS头
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// 处理预检请求
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// 调用下一个处理程序
		next(w, r)
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <node-id> <raft-address> [http-port] [join-address]\n", os.Args[0])
		os.Exit(1)
	}

	nodeID := os.Args[1]
	raftAddr := os.Args[2]

	store := NewStore()
	if err := store.Open(len(os.Args) < 4, nodeID); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	// 设置默认 HTTP 端口
	httpPort := "3000"
	joinAddr := ""

	// 解析可选参数
	if len(os.Args) >= 4 {
		httpPort = os.Args[3]
	}
	if len(os.Args) >= 5 {
		joinAddr = os.Args[4]
	}

	// 使用CORS中间件包装所有HTTP处理函数
	http.HandleFunc("/get", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		value, ok := store.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"key": key, "value": value})
	}))

	http.HandleFunc("/set", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		if key == "" || value == "" {
			http.Error(w, "missing key or value", http.StatusBadRequest)
			return
		}

		if err := store.Set(key, value); err != nil {
			if err.Error() == "not leader" {
				http.Error(w, "not leader", http.StatusBadRequest)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "key": key})
	}))

	http.HandleFunc("/delete", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		if err := store.Delete(key); err != nil {
			if err.Error() == "not leader" {
				http.Error(w, "not leader", http.StatusBadRequest)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "key": key})
	}))

	http.HandleFunc("/all", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		data := store.GetAll()
		json.NewEncoder(w).Encode(data)
	}))

	http.HandleFunc("/raft/stats", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		stats := store.raft.Stats()
		json.NewEncoder(w).Encode(stats)
	}))

	fmt.Printf("Key-Value store server started at :%s (Raft: %s)\n", httpPort, raftAddr)
	fmt.Printf("Join Addr is: %s", joinAddr)
	log.Fatal(http.ListenAndServe(":"+httpPort, nil))
}
