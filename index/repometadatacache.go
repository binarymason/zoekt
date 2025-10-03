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
			maxEntries: getCacheMaxSize(),
		}
	})
	return repoMetadataCache
}

func getMetadataCacheDuration() time.Duration {
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

func setMetadataInCache(key string, repos []*zoekt.Repository, md *zoekt.IndexMetadata, expiry time.Duration) {
	cache := getGlobalMetadataCache()
	cache.set(key, repos, md, expiry)
}

func (c *RepoMetadataCache) set(key string, repos []*zoekt.Repository, md *zoekt.IndexMetadata, expiry time.Duration) {
	if c.disabled() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.evictExpired()
	c.evictToMakeRoom()

	var expiresAt *time.Time
	if expiry > 0 {
		t := time.Now().Add(expiry)
		expiresAt = &t
	}

	c.entries[key] = &RepoMetaCacheEntry{
		Repos:         repos,
		IndexMetadata: md,
		ExpiresAt:     expiresAt,
	}
}

func fetchMetadataFromCache(key string) ([]*zoekt.Repository, *zoekt.IndexMetadata) {
	cache := getGlobalMetadataCache()
	entry, exists := cache.fetch(key)
	if !exists {
		return nil, nil
	}

	return entry.Repos, entry.IndexMetadata
}

func (c *RepoMetadataCache) fetch(key string) (*RepoMetaCacheEntry, bool) {
	if c.disabled() {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]
	if exists && !isStale(entry) {
		return entry, true
	}

	return nil, false
}

func invalidateMetadataCache(key string) {
	cache := getGlobalMetadataCache()
	cache.remove(key)
}

func (c *RepoMetadataCache) remove(key string) {
	if c.disabled() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
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
	cache.clear()
}

func (c *RepoMetadataCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*RepoMetaCacheEntry)
}
