package index

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sourcegraph/zoekt"
)

type RepoMetadataCache struct {
	mu         sync.RWMutex
	entries    map[string]*RepoMetaCacheEntry // Key: shard file path
	expiry     time.Duration
	maxEntries int
}

type RepoMetaCacheEntry struct {
	Repos         []*zoekt.Repository
	IndexMetadata *zoekt.IndexMetadata
	ExpiresAt     *time.Time
}

var (
	repoMetadataCache          *RepoMetadataCache
	repoMetadataCacheSingleton sync.Once
)

func getGlobalMetadataCache() *RepoMetadataCache {
	repoMetadataCacheSingleton.Do(func() {
		repoMetadataCache = &RepoMetadataCache{
			entries:    make(map[string]*RepoMetaCacheEntry),
			expiry:     getRepoMetadataCacheDuration(),
			maxEntries: getCacheMaxSize(),
		}
	})
	return repoMetadataCache
}

func getRepoMetadataCacheDuration() time.Duration {
	expiryStr, ok := os.LookupEnv("ZOEKT_REPOMETADATA_CACHE_EXPIRY")
	if !ok || expiryStr == "" {
		return 0
	}

	d, err := time.ParseDuration(expiryStr)
	if err != nil {
		return 0
	}
	return d
}

func getCacheMaxSize() int {
	entriesStr, ok := os.LookupEnv("ZOEKT_REPOMETADATA_CACHE_ENTRIES")
	if !ok || entriesStr == "" {
		return 0 // disabled
	}

	entries, err := strconv.Atoi(entriesStr)
	if err != nil {
		return 0 // disabled
	}

	return entries
}

func (c *RepoMetadataCache) disabled() bool {
	return c.maxEntries == 0
}

func setMetadataInCache(key string, repos []*zoekt.Repository, md *zoekt.IndexMetadata, expirySeconds int) {
	cache := getGlobalMetadataCache()
	if cache.disabled() {
		return
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.evictExpired()
	cache.evictToMakeRoom()

	var expiresAtPtr *time.Time
	if expirySeconds > 0 {
		t := time.Now().Add(time.Duration(expirySeconds) * time.Second)
		expiresAtPtr = &t
	}
	cache.entries[key] = &RepoMetaCacheEntry{
		Repos:         repos,
		IndexMetadata: md,
		ExpiresAt:     expiresAtPtr,
	}
}

func fetchMetadataFromCache(key string) ([]*zoekt.Repository, *zoekt.IndexMetadata, error) {
	cache := getGlobalMetadataCache()
	if cache.disabled() {
		return nil, nil, nil
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	entry, exists := cache.entries[key]
	if exists {
		if isStale(entry) {
			delete(cache.entries, key)
		} else {
			return entry.Repos, entry.IndexMetadata, nil
		}
	}

	return nil, nil, nil
}

func invalidateMetadataCache(key string) {
	cache := getGlobalMetadataCache()
	if cache.disabled() {
		return
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	delete(cache.entries, key)
}

// evictExpired removes all expired entries (must be called with lock held)
func (c *RepoMetadataCache) evictExpired() {
	now := time.Now()
	for path, entry := range c.entries {
		if entry.ExpiresAt != nil && now.After(*entry.ExpiresAt) {
			delete(c.entries, path)
		}
	}
}

// evictToMakeRoom removes entries to make room for new entry (must be called with lock held)
func (c *RepoMetadataCache) evictToMakeRoom() {
	if c.disabled() {
		return
	}

	if c.maxEntries == -1 {
		return // unlimited
	}

	if len(c.entries) < c.maxEntries {
		return
	}

	// Collect all paths for random eviction
	paths := make([]string, 0, len(c.entries))
	for path := range c.entries {
		paths = append(paths, path)
	}

	// Fisher-Yates shuffle for random order
	for i := len(paths) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		paths[i], paths[j] = paths[j], paths[i]
	}

	// Remove entries until under maxEntries
	for _, path := range paths {
		if len(c.entries) < c.maxEntries {
			break
		}
		delete(c.entries, path)
	}
}

func isStale(entry *RepoMetaCacheEntry) bool {
	if entry.ExpiresAt == nil {
		return false // never expires
	}
	return time.Now().After(*entry.ExpiresAt)
}

// ClearRepoMetadataCache clears all cached entries
func ClearRepoMetadataCache() {
	cache := getGlobalMetadataCache()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.entries = make(map[string]*RepoMetaCacheEntry)
}
