package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	sglog "github.com/sourcegraph/log"

	"github.com/sourcegraph/zoekt"
	configv1 "github.com/sourcegraph/zoekt/cmd/zoekt-sourcegraph-indexserver/grpc/protos/sourcegraph/zoekt/configuration/v1"
	"github.com/sourcegraph/zoekt/index"
	"github.com/sourcegraph/zoekt/internal/ctags"
)

const defaultIndexingTimeout = 1*time.Hour + 30*time.Minute

// IndexOptions are the options that Sourcegraph can set via it's search
// configuration endpoint.
type IndexOptions struct {
	// LargeFiles is a slice of glob patterns where matching file paths should
	// be indexed regardless of their size. The pattern syntax can be found
	// here: https://golang.org/pkg/path/filepath/#Match.
	LargeFiles []string

	// Symbols if true will make zoekt index the output of ctags.
	Symbols bool

	// Branches is a slice of branches to index.
	Branches []zoekt.RepositoryBranch

	// RepoID is the Sourcegraph Repository ID.
	RepoID uint32

	// Name is the Repository Name.
	Name string

	// CloneURL is the internal clone URL for Name.
	CloneURL string

	// Priority indicates ranking in results, higher first.
	Priority float64

	// Public is true if the repository is public.
	Public bool

	// Fork is true if the repository is a fork.
	Fork bool

	// Archived is true if the repository is archived.
	Archived bool

	// Map from language to scip-ctags, universal-ctags, or neither
	LanguageMap ctags.LanguageMap

	// The number of threads to use for indexing shards. Defaults to the number of available
	// CPUs. If the server flag -cpu_fraction is set, then this value overrides it.
	ShardConcurrency int32

	// TenantID is the tenant ID for the repository.
	TenantID int
}

// indexArgs represents the arguments we pass to zoekt-git-index
type indexArgs struct {
	IndexOptions

	// Incremental indicates to skip indexing if already indexed.
	Incremental bool

	// IndexDir is the index directory to store the shards.
	IndexDir string

	// Parallelism is the number of shards to compute in parallel.
	Parallelism int

	// FileLimit is the maximum size of a file
	FileLimit int

	// UseDelta is true if we want to use the new delta indexer. This should
	// only be true for repositories we explicitly enable.
	UseDelta bool

	// DeltaShardNumberFallbackThreshold is an upper limit on the number of preexisting shards that can exist
	// before attempting a delta build.
	DeltaShardNumberFallbackThreshold uint64

	// ShardMerging is true if we want zoekt-git-index to respect compound shards.
	ShardMerging bool
}

// BuildOptions returns a index.Options represented by indexArgs. Note: it
// doesn't set fields like repository/branch.
func (o *indexArgs) BuildOptions() *index.Options {
	return &index.Options{
		// It is important that this RepositoryDescription exactly matches what
		// the indexer we call will produce. This is to ensure that
		// IncrementalSkipIndexing and IndexState can correctly calculate if
		// nothing needs to be done.
		RepositoryDescription: zoekt.Repository{
			TenantID: o.TenantID,
			ID:       o.RepoID,
			Name:     o.Name,
			Branches: o.Branches,
			RawConfig: map[string]string{
				"repoid":   strconv.Itoa(int(o.RepoID)),
				"priority": strconv.FormatFloat(o.Priority, 'g', -1, 64),
				"public":   marshalBool(o.Public),
				"fork":     marshalBool(o.Fork),
				"archived": marshalBool(o.Archived),
				// Calculate repo rank based on the latest commit date.
				"latestCommitDate": "1",
				"tenantID":         strconv.Itoa(o.TenantID),
			},
		},
		IndexDir:         o.IndexDir,
		Parallelism:      o.Parallelism,
		SizeMax:          o.FileLimit,
		LargeFiles:       o.LargeFiles,
		CTagsMustSucceed: o.Symbols,
		DisableCTags:     !o.Symbols,
		IsDelta:          o.UseDelta,

		LanguageMap: o.LanguageMap,

		ShardMerging: o.ShardMerging,
	}
}

func marshalBool(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func (o *indexArgs) String() string {
	s := fmt.Sprintf("%d %s", o.RepoID, o.Name)
	for i, b := range o.Branches {
		if i == 0 {
			s = fmt.Sprintf("%s@%s=%s", s, b.Name, b.Version)
		} else {
			s = fmt.Sprintf("%s,%s=%s", s, b.Name, b.Version)
		}
	}
	return s
}

type gitIndexConfig struct {
	// runCmd is the function that's used to execute all external commands (such as calls to "git" or "zoekt-git-index")
	// that gitIndex may construct.
	runCmd func(*exec.Cmd) error

	// findRepositoryMetadata is the function that returns the repository metadata for the
	// repository specified in args. 'ok' is false if the repository's metadata
	// couldn't be found or if an error occurred.
	//
	// The primary purpose of this configuration option is to be able to provide a stub
	// implementation for this in our test suite. All other callers should use build.Options.FindRepositoryMetadata().
	findRepositoryMetadata func(args *indexArgs) (repository *zoekt.Repository, metadata *zoekt.IndexMetadata, ok bool, err error)

	// timeout defines how long the index server waits before killing an indexing job.
	timeout time.Duration
}

func gitIndex(ctx context.Context, c gitIndexConfig, o *indexArgs, sourcegraph Sourcegraph, l sglog.Logger) error {
	logger := l.Scoped("gitIndex")

	if len(o.Branches) == 0 {
		return errors.New("zoekt-git-index requires 1 or more branches")
	}

	if c.runCmd == nil {
		return errors.New("runCmd in provided configuration was nil - a function must be provided")
	}

	if c.findRepositoryMetadata == nil {
		return errors.New("findRepositoryMetadata in provided configuration was nil - a function must be provided")
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	gitDir, err := tmpGitDir(o.Name)
	if err != nil {
		return err
	}
	defer os.RemoveAll(gitDir) // best-effort cleanup

	err = fetchRepo(ctx, gitDir, o, c, logger)
	if err != nil {
		return err
	}

	err = setZoektConfig(ctx, gitDir, o, c)
	if err != nil {
		return err
	}

	err = indexRepo(ctx, gitDir, sourcegraph, o, c, logger)
	if err != nil {
		return err
	}

	return nil
}

func fetchRepo(ctx context.Context, gitDir string, o *indexArgs, c gitIndexConfig, logger sglog.Logger) error {
	// Create a repo to fetch into
	cmd := exec.CommandContext(ctx, "git",
		// use a random default branch. This is so that HEAD isn't a symref to a
		// branch that is indexed. For example if you are indexing
		// HEAD,master. Then HEAD would be pointing to master by default.
		"-c", "init.defaultBranch=nonExistentBranchBB0FOFCH32",
		"init",
		// we don't need a working copy
		"--bare",
		gitDir)
	cmd.Stdin = &bytes.Buffer{}
	if err := c.runCmd(cmd); err != nil {
		return err
	}

	var fetchDuration time.Duration
	successfullyFetchedCommitsCount := 0
	allFetchesSucceeded := true

	defer func() {
		success := strconv.FormatBool(allFetchesSucceeded)
		name := repoNameForMetric(o.Name)
		metricFetchDuration.WithLabelValues(success, name).Observe(fetchDuration.Seconds())
	}()

	runFetch := func(branches []zoekt.RepositoryBranch) error {
		// We shallow fetch each commit specified in zoekt.Branches. This requires
		// the server to have configured both uploadpack.allowAnySHA1InWant and
		// uploadpack.allowFilter. (See gitservice.go in the Sourcegraph repository)
		fetchArgs := []string{
			"-C", gitDir,
			"-c", "protocol.version=2",
			"-c", "http.extraHeader=X-Sourcegraph-Actor-UID: internal",
			"-c", "http.extraHeader=X-Sourcegraph-Tenant-ID: " + strconv.Itoa(o.TenantID),
			"fetch", "--depth=1", "--no-tags",
		}

		// If there are no exceptions to MaxFileSize (1MB), we can avoid fetching these large files.
		if len(o.LargeFiles) == 0 {
			fetchArgs = append(fetchArgs, "--filter=blob:limit=1m")
		}

		fetchArgs = append(fetchArgs, o.CloneURL)

		var commits []string
		for _, b := range branches {
			commits = append(commits, b.Version)
		}

		fetchArgs = append(fetchArgs, commits...)

		cmd = exec.CommandContext(ctx, "git", fetchArgs...)
		cmd.Stdin = &bytes.Buffer{}

		start := time.Now()
		err := c.runCmd(cmd)
		fetchDuration += time.Since(start)

		if err != nil {
			allFetchesSucceeded = false
			var bs []string
			for _, b := range branches {
				bs = append(bs, b.String())
			}

			formattedBranches := strings.Join(bs, ", ")
			return fmt.Errorf("fetching %s: %w", formattedBranches, err)
		}

		successfullyFetchedCommitsCount += len(commits)
		return nil
	}

	fetchPriorAndLatestCommits := func() error {
		prior, err := priorBranches(c, o)
		if err != nil {
			return err
		}

		var allBranches []zoekt.RepositoryBranch
		allBranches = append(allBranches, o.Branches...)
		allBranches = append(allBranches, prior...)

		return runFetch(allBranches)
	}

	fetchOnlyLatestCommits := func() error {
		return runFetch(o.Branches)
	}

	if o.UseDelta {
		err := fetchPriorAndLatestCommits()
		if err != nil {
			name := o.BuildOptions().RepositoryDescription.Name
			id := o.BuildOptions().RepositoryDescription.ID

			errorLog.Printf("delta build: failed to prepare delta build for %q (ID %d): failed to fetch both latest and prior commits: %s", name, id, err)
			err = fetchOnlyLatestCommits()
			if err != nil {
				return err
			}
		}
	} else {
		err := fetchOnlyLatestCommits()
		if err != nil {
			return err
		}
	}

	// We then create the relevant refs for each fetched commit.
	for _, b := range o.Branches {
		ref := b.Name
		if ref != "HEAD" {
			ref = "refs/heads/" + ref
		}
		cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "update-ref", ref, b.Version)
		cmd.Stdin = &bytes.Buffer{}
		if err := c.runCmd(cmd); err != nil {
			return fmt.Errorf("failed update-ref %s to %s: %w", ref, b.Version, err)
		}
	}

	logger.Debug("successfully fetched git data",
		sglog.String("repo", o.Name),
		sglog.Uint32("id", o.RepoID),
		sglog.Int("commits_count", successfullyFetchedCommitsCount),
		sglog.Duration("duration", fetchDuration),
	)
	return nil
}

func setZoektConfig(ctx context.Context, gitDir string, o *indexArgs, c gitIndexConfig) error {
	// create git configuration with options
	type configKV struct{ Key, Value string }
	config := []configKV{{
		// zoekt.name is used by zoekt-git-index to set the repository name.
		Key:   "name",
		Value: o.Name,
	}}
	for k, v := range o.BuildOptions().RepositoryDescription.RawConfig {
		config = append(config, configKV{Key: k, Value: v})
	}
	sort.Slice(config, func(i, j int) bool {
		return config[i].Key < config[j].Key
	})

	// write git configuration to repo
	for _, kv := range config {
		cmd := exec.CommandContext(ctx, "git", "-C", gitDir, "config", "zoekt."+kv.Key, kv.Value)
		cmd.Stdin = &bytes.Buffer{}
		if err := c.runCmd(cmd); err != nil {
			return err
		}
	}
	return nil
}

func indexRepo(ctx context.Context, gitDir string, sourcegraph Sourcegraph, o *indexArgs, c gitIndexConfig, logger sglog.Logger) error {
	args := []string{
		"-submodules=false",
	}

	// Even though we check for incremental in this process, we still pass it
	// in just in case we regress in how we check in process. We will still
	// notice thanks to metrics and increased load on gitserver.
	if o.Incremental {
		args = append(args, "-incremental")
	}

	var branches []string
	for _, b := range o.Branches {
		branches = append(branches, b.Name)
	}
	args = append(args, "-branches", strings.Join(branches, ","))

	if o.UseDelta {
		args = append(args, "-delta")
		args = append(args, "-delta_threshold", strconv.FormatUint(o.DeltaShardNumberFallbackThreshold, 10))
	}

	if len(o.LanguageMap) > 0 {
		var languageMap []string
		for language, parser := range o.LanguageMap {
			languageMap = append(languageMap, language+":"+ctags.ParserToString(parser))
		}
		args = append(args, "-language_map", strings.Join(languageMap, ","))
	}

	args = append(args, o.BuildOptions().Args()...)
	args = append(args, gitDir)

	cmd := exec.CommandContext(ctx, "zoekt-git-index", args...)
	cmd.Stdin = &bytes.Buffer{}
	if err := c.runCmd(cmd); err != nil {
		return err
	}
	return nil
}

func priorBranches(c gitIndexConfig, o *indexArgs) ([]zoekt.RepositoryBranch, error) {
	existingRepository, _, found, err := c.findRepositoryMetadata(o)
	if err != nil {
		return nil, fmt.Errorf("loading repository metadata: %w", err)
	}

	if !found || len(existingRepository.Branches) == 0 {
		return nil, fmt.Errorf("no prior shards found")
	}

	return existingRepository.Branches, nil
}

func tmpGitDir(name string) (string, error) {
	abs := url.QueryEscape(name)
	if len(abs) > 200 {
		h := sha1.New()
		_, _ = io.WriteString(h, abs)
		abs = abs[:200] + fmt.Sprintf("%x", h.Sum(nil))[:8]
	}
	dir := filepath.Join(os.TempDir(), abs+".git")
	if _, err := os.Stat(dir); err == nil {
		if err := os.RemoveAll(dir); err != nil {
			return "", err
		}
	}
	return dir, nil
}

// FromProto converts a ZoektIndexOptions proto message into an IndexOptions struct.
func (o *IndexOptions) FromProto(x *configv1.ZoektIndexOptions) {
	branches := make([]zoekt.RepositoryBranch, 0, len(x.Branches))
	for _, b := range x.GetBranches() {
		branches = append(branches, zoekt.RepositoryBranch{
			Name:    b.GetName(),
			Version: b.GetVersion(),
		})
	}

	languageMap := make(map[string]ctags.CTagsParserType)
	for _, lang := range x.GetLanguageMap() {
		languageMap[lang.GetLanguage()] = ctags.CTagsParserType(lang.GetCtags().Number())
	}

	*o = IndexOptions{
		RepoID:     uint32(x.GetRepoId()),
		LargeFiles: x.GetLargeFiles(),
		Symbols:    x.GetSymbols(),
		Branches:   branches,
		Name:       x.GetName(),

		Priority: x.GetPriority(),

		Public:   x.GetPublic(),
		Fork:     x.GetFork(),
		Archived: x.GetArchived(),

		LanguageMap:      languageMap,
		ShardConcurrency: x.GetShardConcurrency(),

		TenantID: int(x.TenantId),
	}
}
