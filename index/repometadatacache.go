package index

import (
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sourcegraph/zoekt"
)

type RepoMetadataCache struct {
	mu       sync.RWMutex
	entries  map[string]*RepoMetaCacheEntry // Key: shard file path
	expiry   time.Duration
	maxBytes int64
	curBytes int64
}

type RepoMetaCacheEntry struct {
	Repos         []*zoekt.Repository
	IndexMetadata *zoekt.IndexMetadata
	CachedAt      time.Time
	SizeBytes     int64 // Estimated memory usage
}

var (
	repoMetadataCache          *RepoMetadataCache
	repoMetadataCacheSingleton sync.Once
)

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

func getCacheMaxSize() int64 {
	sizeStr, ok := os.LookupEnv("ZOEKT_REPOMETADATA_CACHE_SIZE")
	if !ok || sizeStr == "" {
		return 0 // unlimited
	}

	// Parse sizes like "1gb", "500mb", "100KB"
	sizeStr = strings.ToUpper(strings.TrimSpace(sizeStr))
	multiplier := int64(1)

	if strings.HasSuffix(sizeStr, "GB") {
		multiplier = 1 << 30
		sizeStr = strings.TrimSuffix(sizeStr, "GB")
	} else if strings.HasSuffix(sizeStr, "MB") {
		multiplier = 1 << 20
		sizeStr = strings.TrimSuffix(sizeStr, "MB")
	} else if strings.HasSuffix(sizeStr, "KB") {
		multiplier = 1 << 10
		sizeStr = strings.TrimSuffix(sizeStr, "KB")
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0
	}
	return size * multiplier
}

func estimateEntrySize(repos []*zoekt.Repository) int64 {
	// Rough estimate:
	// - Each repo ~1KB base + metadata size
	// - IndexMetadata ~500 bytes
	size := int64(500) // base for IndexMetadata
	for _, repo := range repos {
		size += 1024 // base repo size
		for k, v := range repo.Metadata {
			size += int64(len(k) + len(v))
		}
	}
	return size
}

func getGlobalCache() *RepoMetadataCache {
	repoMetadataCacheSingleton.Do(func() {
		repoMetadataCache = &RepoMetadataCache{
			entries:  make(map[string]*RepoMetaCacheEntry),
			expiry:   getRepoMetadataCacheDuration(),
			maxBytes: getCacheMaxSize(),
		}
	})
	return repoMetadataCache
}

// readMetadataPathWithCache is a cache wrapper around readMetadataPathUncached
func readMetadataPathWithCache(p string) ([]*zoekt.Repository, *zoekt.IndexMetadata, error) {
	cache := getGlobalCache()

	// If caching is disabled (expiry == 0), go straight to disk
	if cache.expiry == 0 {
		return readMetadataPathUncached(p)
	}

	cache.mu.RLock()
	entry, exists := cache.entries[p]
	cache.mu.RUnlock()

	// Return cache hit if valid
	if exists && !isStale(cache, entry) {
		return entry.Repos, entry.IndexMetadata, nil
	}

	// Read from disk if cache miss or stale
	repos, metadata, err := readMetadataPathUncached(p)
	if err != nil {
		return nil, nil, err
	}

	// Calculate size of new entry
	entrySize := estimateEntrySize(repos)

	// Update cache with eviction if needed
	cache.mu.Lock()
	defer cache.mu.Unlock()

	// Remove expired entries first
	cache.evictExpired()

	// If adding this would exceed limit, evict entries
	if cache.maxBytes > 0 && cache.curBytes+entrySize > cache.maxBytes {
		cache.evictToMakeRoom(entrySize)
	}

	// Add/update the entry
	if oldEntry, exists := cache.entries[p]; exists {
		cache.curBytes -= oldEntry.SizeBytes
	}

	cache.entries[p] = &RepoMetaCacheEntry{
		Repos:         repos,
		IndexMetadata: metadata,
		CachedAt:      time.Now(),
		SizeBytes:     entrySize,
	}
	cache.curBytes += entrySize

	return repos, metadata, nil
}

// evictExpired removes all expired entries (must be called with lock held)
func (c *RepoMetadataCache) evictExpired() {
	now := time.Now()
	for path, entry := range c.entries {
		if now.Sub(entry.CachedAt) > c.expiry {
			c.curBytes -= entry.SizeBytes
			delete(c.entries, path)
		}
	}
}

// evictToMakeRoom removes entries to make room for new entry (must be called with lock held)
func (c *RepoMetadataCache) evictToMakeRoom(needed int64) {
	// If the new entry is larger than max cache size, we can't cache it
	if needed > c.maxBytes {
		return
	}

	// Collect all paths for random eviction
	paths := make([]string, 0, len(c.entries))
	for path := range c.entries {
		paths = append(paths, path)
	}

	// Randomly evict until we have enough space
	// Fisher-Yates shuffle for random order
	for i := len(paths) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		paths[i], paths[j] = paths[j], paths[i]
	}

	for _, path := range paths {
		if c.curBytes+needed <= c.maxBytes {
			break
		}
		entry := c.entries[path]
		c.curBytes -= entry.SizeBytes
		delete(c.entries, path)
	}
}

func isStale(cache *RepoMetadataCache, entry *RepoMetaCacheEntry) bool {
	if cache.expiry == 0 {
		return true
	}
	return time.Since(entry.CachedAt) > cache.expiry
}

// ClearRepoMetadataCache clears all cached entries
func ClearRepoMetadataCache() {
	cache := getGlobalCache()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.entries = make(map[string]*RepoMetaCacheEntry)
	cache.curBytes = 0
}

// GetCacheStats returns statistics about the cache
func GetCacheStats() (entries int, bytesUsed int64, maxBytes int64) {
	cache := getGlobalCache()
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return len(cache.entries), cache.curBytes, cache.maxBytes
}
