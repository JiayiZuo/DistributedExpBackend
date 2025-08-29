package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

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

func main() {
	store := NewKVStore()

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
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
	})

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
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
		store.Set(key, value)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "key": key})
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		store.Delete(key)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "key": key})
	})

	http.HandleFunc("/all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		data := store.GetAll()
		json.NewEncoder(w).Encode(data)
	})

	fmt.Println("Key-Value store server started at :3000")
	log.Fatal(http.ListenAndServe(":3000", nil))
}
