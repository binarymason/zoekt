package index

import (
	"testing"
	"time"

	"github.com/sourcegraph/zoekt"
)

func Test_getMetadataCacheDuration(t *testing.T) {
	t.Setenv("ZOEKT_REPOMETADATA_CACHE_EXPIRY", "2s")
	d := getMetadataCacheDuration()
	if d != 2*time.Second {
		t.Errorf("expected 2s, got %v", d)
	}

	t.Setenv("ZOEKT_REPOMETADATA_CACHE_EXPIRY", "invalid")
	if getMetadataCacheDuration() != 0 {
		t.Errorf("expected 0 for invalid duration")
	}
}

func Test_getCacheMaxSize(t *testing.T) {
	t.Setenv("ZOEKT_REPOMETADATA_CACHE_ENTRIES", "5")
	if got := getCacheMaxSize(); got != 5 {
		t.Errorf("expected 5, got %d", got)
	}

	t.Setenv("ZOEKT_REPOMETADATA_CACHE_ENTRIES", "invalid")
	if getCacheMaxSize() != 0 {
		t.Errorf("expected 0 for invalid value")
	}
}

func newTestCache(maxEntries int) *RepoMetadataCache {
	return &RepoMetadataCache{
		entries:    make(map[string]*RepoMetaCacheEntry),
		maxEntries: maxEntries,
	}
}

func TestCacheSetFetchRemove(t *testing.T) {
	cache := newTestCache(2)
	key := "shard1"
	repos := []*zoekt.Repository{{Name: "repo1"}}
	md := &zoekt.IndexMetadata{IndexFormatVersion: 1}
	expiry := 1 * time.Second

	cache.set(key, repos, md, expiry)
	entry, ok := cache.fetch(key)
	if !ok || entry == nil {
		t.Fatalf("fetch failed after set")
	}
	if entry.Repos[0].Name != "repo1" {
		t.Errorf("wrong repo name")
	}

	cache.remove(key)
	if _, ok := cache.fetch(key); ok {
		t.Errorf("expected entry to be removed")
	}
}

func TestCacheEvictToMakeRoom(t *testing.T) {
	cache := newTestCache(2)
	for i := 0; i < 3; i++ {
		k := "k" + string(rune('A'+i))
		cache.set(k, []*zoekt.Repository{{Name: k}}, &zoekt.IndexMetadata{}, 0)
	}
	if len(cache.entries) > 2 {
		t.Errorf("evictToMakeRoom failed, entries: %d", len(cache.entries))
	}
}

func TestCacheEvictExpired(t *testing.T) {
	cache := newTestCache(3)
	now := time.Now()
	expired := now.Add(-time.Second)
	valid := now.Add(time.Hour)
	cache.entries["expired"] = &RepoMetaCacheEntry{ExpiresAt: &expired}
	cache.entries["valid"] = &RepoMetaCacheEntry{ExpiresAt: &valid}
	cache.evictExpired()
	if _, ok := cache.entries["expired"]; ok {
		t.Errorf("expired entry not evicted")
	}
	if _, ok := cache.entries["valid"]; !ok {
		t.Errorf("valid entry wrongly evicted")
	}
}
