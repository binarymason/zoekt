// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command zoekt-sourcegraph-indexserver periodically reindexes repositories
// from a Sourcegraph instance. It uses a "pull-based" design, where it periodically
// reaches out to the Sourcegraph instance for the list of repositories to reindex.
package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"github.com/peterbourgon/ff/v3/ffcli"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	sglog "github.com/sourcegraph/log"
	"github.com/sourcegraph/mountinfo"

	"github.com/sourcegraph/zoekt"
	configv1 "github.com/sourcegraph/zoekt/cmd/zoekt-sourcegraph-indexserver/grpc/protos/sourcegraph/zoekt/configuration/v1"
	indexserverv1 "github.com/sourcegraph/zoekt/cmd/zoekt-sourcegraph-indexserver/grpc/protos/zoekt/indexserver/v1"
	"github.com/sourcegraph/zoekt/grpc/defaults"
	"github.com/sourcegraph/zoekt/grpc/grpcutil"
	"github.com/sourcegraph/zoekt/grpc/internalerrs"
	"github.com/sourcegraph/zoekt/grpc/messagesize"
	"github.com/sourcegraph/zoekt/index"
	"github.com/sourcegraph/zoekt/internal/debugserver"
	"github.com/sourcegraph/zoekt/internal/profiler"
	"github.com/sourcegraph/zoekt/internal/tenant"

	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/multierr"
	"golang.org/x/net/trace"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	metricResolveRevisionsDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "resolve_revisions_seconds",
		Help:    "A histogram of latencies for resolving all repository revisions.",
		Buckets: prometheus.ExponentialBuckets(1, 10, 6), // 1s -> 27min
	})

	metricResolveRevisionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "resolve_revision_seconds",
		Help:    "A histogram of latencies for resolving a repository revision.",
		Buckets: prometheus.ExponentialBuckets(.25, 2, 4), // 250ms -> 2s
	}, []string{"success"}) // success=true|false

	metricGetIndexOptions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "get_index_options_total",
		Help: "The total number of times we tried to get index options for a repository. Includes errors.",
	})
	metricGetIndexOptionsError = promauto.NewCounter(prometheus.CounterOpts{
		Name: "get_index_options_error_total",
		Help: "The total number of times we failed to get index options for a repository.",
	})

	metricIndexDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "index_repo_seconds",
		Help: "A histogram of latencies for indexing a repository.",
		Buckets: prometheus.ExponentialBucketsRange(
			(100 * time.Millisecond).Seconds(),
			(40*time.Minute + defaultIndexingTimeout).Seconds(), // add an extra 40 minutes to account for the time it takes to clone the repo
			20),
	}, []string{
		"state", // state is an indexState
		"name",  // name of the repository that was indexed
	})

	metricIndexingDelay = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "index_indexing_delay_seconds",
		Help:    "A histogram of durations from when an index job is added to the queue, to the time it completes.",
		Buckets: prometheus.ExponentialBuckets(60, 2, 14), // 1 minute -> 5.5 days
	}, []string{
		"state", // state is an indexState
		"name",  // the name of the repository that was indexed
	})

	metricFetchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "index_fetch_seconds",
		Help:    "A histogram of latencies for fetching a repository.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30, 60, 180, 300, 600}, // 50ms -> 10 minutes
	}, []string{
		"success", // true|false
		"name",    // the name of the repository that the commits were fetched from
	})

	metricIndexIncrementalIndexState = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "index_incremental_index_state",
		Help: "A count of the state on disk vs what we want to build. See zoekt/build.IndexState.",
	}, []string{"state"}) // state is index.IndexState

	metricNumIndexed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "index_num_indexed",
		Help: "Number of indexed repos by code host",
	})

	metricFailingTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "index_failing_total",
		Help: "Counts failures to index (indexing activity, should be used with rate())",
	})

	metricIndexingTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "index_indexing_total",
		Help: "Counts indexings (indexing activity, should be used with rate())",
	})

	metricNumStoppedTrackingTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "index_num_stopped_tracking_total",
		Help: "Counts the number of repos we stopped tracking.",
	})

	// clientMetricsOnce returns a singleton instance of the client metrics
	// that are shared across all gRPC clients that this process creates.
	//
	// This function panics if the metrics cannot be registered with the default
	// Prometheus registry.
	clientMetricsOnce = sync.OnceValue(func() *grpcprom.ClientMetrics {
		clientMetrics := grpcprom.NewClientMetrics(
			grpcprom.WithClientCounterOptions(),
			grpcprom.WithClientHandlingTimeHistogram(), // record the overall request latency for a gRPC request
			grpcprom.WithClientStreamRecvHistogram(),   // record how long it takes for a client to receive a message during a streaming RPC
			grpcprom.WithClientStreamSendHistogram(),   // record how long it takes for a client to send a message during a streaming RPC
		)
		prometheus.DefaultRegisterer.MustRegister(clientMetrics)
		return clientMetrics
	})
)

// 1 MB; match https://sourcegraph.sgdev.org/github.com/sourcegraph/sourcegraph/-/blob/cmd/symbols/internal/symbols/search.go#L22
// NOTE: if you change this, you must also update gitIndex to use the same value when fetching the repo.
const MaxFileSize = 1 << 20

// set of repositories that we want to capture separate indexing metrics for
var reposWithSeparateIndexingMetrics = make(map[string]struct{})

type indexState string

const (
	indexStateFail        indexState = "fail"
	indexStateSuccess     indexState = "success"
	indexStateSuccessMeta indexState = "success_meta" // We only updated metadata
	indexStateNoop        indexState = "noop"         // We didn't need to update index
	indexStateEmpty       indexState = "empty"        // index is empty (empty repo)
)

// Server is the main functionality of zoekt-sourcegraph-indexserver. It
// exists to conveniently use all the options passed in via func main.
type Server struct {
	logger sglog.Logger

	Sourcegraph Sourcegraph

	// rootURL is the root URL of the Sourcegraph instance.
	rootURL *url.URL

	BatchSize int

	// IndexDir is the index directory to use.
	IndexDir string

	// IndexConcurrency is the number of repositories we index at once.
	IndexConcurrency int

	// indexSemaphore limits the number of concurrent index operations
	indexSemaphore chan struct{}

	// Interval is how often we sync with Sourcegraph.
	Interval time.Duration

	// CPUCount is the number of CPUs to use for indexing shards.
	CPUCount int

	queue Queue

	// muIndexDir protects the index directory from concurrent access.
	muIndexDir indexMutex

	// If true, shard merging is enabled.
	shardMerging bool

	// deltaBuildRepositoriesAllowList is an allowlist for repositories that we
	// use delta-builds for instead of normal builds
	deltaBuildRepositoriesAllowList map[string]struct{}

	// deltaShardNumberFallbackThreshold is an upper limit on the number of preexisting shards that can exist
	// before attempting a delta build.
	deltaShardNumberFallbackThreshold uint64

	// repositoriesSkipSymbolsCalculationAllowList is an allowlist for repositories that
	// we skip calculating symbols metadata for during builds
	repositoriesSkipSymbolsCalculationAllowList map[string]struct{}

	hostname string

	mergeOpts mergeOpts

	// timeout defines how long the index server waits before killing an indexing job.
	timeout time.Duration

	indexserverv1.UnimplementedSourcegraphIndexserverServiceServer
}

var (
	debugLog = log.New(io.Discard, "[DEBUG] ", log.LstdFlags)
	infoLog  = log.New(os.Stderr, "[INFO] ", log.LstdFlags)
	errorLog = log.New(os.Stderr, "[ERROR] ", log.LstdFlags)
)

// our index commands should output something every 100mb they process.
//
// 2020-11-24 Keegan. "This should be rather quick so 5m is more than enough
// time."  famous last words. A client was indexing a monorepo with 42
// cores... 5m was not enough.
const noOutputTimeout = 30 * time.Minute

func (s *Server) loggedRun(tr trace.Trace, cmd *exec.Cmd) (err error) {
	out := &synchronizedBuffer{}
	cmd.Stdout = out
	cmd.Stderr = out

	tr.LazyPrintf("%s", cmd.Args)

	defer func() {
		if err != nil {
			outS := out.String()
			tr.LazyPrintf("failed: %v", err)
			tr.LazyPrintf("output: %s", out)
			tr.SetError()
			err = fmt.Errorf("command %s failed: %v\nOUT: %s", cmd.Args, err, outS)
		}
	}()

	s.logger.Debug("logged run", sglog.Strings("args", cmd.Args))

	if err := cmd.Start(); err != nil {
		return err
	}

	errC := make(chan error)
	go func() {
		errC <- cmd.Wait()
	}()

	// This channel is set after we have sent sigquit. It allows us to follow up
	// with a sigkill if the process doesn't quit after sigquit.
	kill := make(<-chan time.Time)

	lastLen := 0
	for {
		select {
		case <-time.After(noOutputTimeout):
			// Periodically check if we have had output. If not kill the process.
			if out.Len() != lastLen {
				lastLen = out.Len()
				infoLog.Printf("still running %s", cmd.Args)
			} else {
				// Send quit (C-\) first so we get a stack dump.
				infoLog.Printf("no output for %s, quitting %s", noOutputTimeout, cmd.Args)
				if err := cmd.Process.Signal(unix.SIGQUIT); err != nil {
					errorLog.Println("quit failed:", err)
				}

				// send sigkill if still running in 10s
				kill = time.After(10 * time.Second)
			}

		case <-kill:
			infoLog.Printf("still running, killing %s", cmd.Args)
			if err := cmd.Process.Kill(); err != nil {
				errorLog.Println("kill failed:", err)
			}

		case err := <-errC:
			if err != nil {
				return err
			}

			tr.LazyPrintf("success")
			return nil
		}
	}
}

// synchronizedBuffer wraps a strings.Builder with a mutex. Used so we can
// monitor the buffer while it is being written to.
type synchronizedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (sb *synchronizedBuffer) Write(p []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.b.Write(p)
}

func (sb *synchronizedBuffer) Len() int {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.b.Len()
}

func (sb *synchronizedBuffer) String() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.b.String()
}

// pauseFileName if present in IndexDir will stop index jobs from
// running. This is to make it possible to experiment with the content of the
// IndexDir without the indexserver writing to it.
const (
	pauseFileName = "PAUSE"
	pauseEnvVar   = "SRC_PAUSE_INDEXING"
)

// isIndexingPaused checks if indexing should be paused based on either:
// 1. The presence of a PAUSE file in the index directory
// 2. The SRC_PAUSE_INDEXING environment variable being set to true
func isIndexingPaused(indexDir string) (bool, string) {
	// Check for PAUSE file first
	if b, err := os.ReadFile(filepath.Join(indexDir, pauseFileName)); err == nil {
		return true, fmt.Sprintf("indexserver manually paused via PAUSE file: %s", string(bytes.TrimSpace(b)))
	}

	// Then check environment variable
	if getEnvWithDefaultBool(pauseEnvVar, false) {
		return true, fmt.Sprintf("indexserver paused via %s environment variable", pauseEnvVar)
	}

	return false, ""
}

// Run the sync loop. This blocks forever.
func (s *Server) Run() {
	removeIncompleteShards(s.IndexDir)

	// Start a goroutine which updates the queue with commits to index.
	go func() {
		// We update the list of indexed repos every Interval. To speed up manual
		// testing we also listen for SIGUSR1 to trigger updates.
		// "pkill -SIGUSR1 zoekt-sourcegra"
		for range jitterTicker(s.Interval, unix.SIGUSR1) {
			if paused, msg := isIndexingPaused(s.IndexDir); paused {
				infoLog.Printf(msg)
				continue
			}

			repos, err := s.Sourcegraph.List(context.Background(), listIndexed(s.IndexDir))
			if err != nil {
				errorLog.Printf("error listing repos: %s", err)
				continue
			}

			debugLog.Printf("updating index queue with %d repositories", len(repos.IDs))

			// Stop indexing repos we don't need to track anymore
			removed := s.queue.MaybeRemoveMissing(repos.IDs)
			metricNumStoppedTrackingTotal.Add(float64(len(removed)))
			if len(removed) > 0 {
				infoLog.Printf("stopped tracking %d repositories: %s", len(removed), formatListUint32(removed, 5))
			}

			cleanupDone := make(chan struct{})
			go func() {
				defer close(cleanupDone)
				s.muIndexDir.Global(func() {
					cleanup(s.IndexDir, repos.IDs, time.Now(), s.shardMerging)
				})
			}()

			repos.IterateIndexOptions(s.queue.AddOrUpdate)

			// IterateIndexOptions will only iterate over repositories that have
			// changed since we last called list. However, we want to add all IDs
			// back onto the queue just to check that what is on disk is still
			// correct. This will use the last IndexOptions we stored in the
			// queue. The repositories not on the queue (missing) need a forced
			// fetch of IndexOptions.
			missing := s.queue.Bump(repos.IDs)
			s.Sourcegraph.ForceIterateIndexOptions(s.queue.AddOrUpdate, func(uint32, error) {}, missing...)

			setShardsCounter(s.IndexDir)

			<-cleanupDone
		}
	}()

	go func() {
		for range jitterTicker(s.mergeOpts.vacuumInterval, unix.SIGUSR1) {
			if s.shardMerging {
				s.vacuum()
			}
		}
	}()

	go func() {
		for range jitterTicker(s.mergeOpts.mergeInterval, unix.SIGUSR1) {
			if s.shardMerging {
				s.doMerge()
			}
		}
	}()

	for range s.IndexConcurrency {
		go s.processQueue()
	}

	// block forever
	select {}
}

// formatList returns a comma-separated list of the first min(len(v), m) items.
func formatListUint32(v []uint32, m int) string {
	if len(v) < m {
		m = len(v)
	}

	sb := strings.Builder{}
	for i := range m {
		fmt.Fprintf(&sb, "%d, ", v[i])
	}

	if len(v) > m {
		sb.WriteString("...")
	}

	return strings.TrimRight(sb.String(), ", ")
}

func (s *Server) processQueue() {
	for {
		if paused, _ := isIndexingPaused(s.IndexDir); paused {
			time.Sleep(time.Second)
			continue
		}

		item, ok := s.queue.Pop()
		if !ok {
			time.Sleep(time.Second)
			continue
		}

		opts := item.Opts
		args := s.indexArgs(opts)

		ran := s.muIndexDir.With(opts.Name, func() {
			// only record time taken once we hold the lock. This avoids us
			// recording time taken while merging/cleanup runs.
			start := time.Now()

			state, err := s.index(context.Background(), args)

			elapsed := time.Since(start)
			metricIndexDuration.WithLabelValues(string(state), repoNameForMetric(opts.Name)).Observe(elapsed.Seconds())

			indexDelay := time.Since(item.DateAddedToQueue)
			metricIndexingDelay.WithLabelValues(string(state), repoNameForMetric(opts.Name)).Observe(indexDelay.Seconds())

			if err != nil {
				errorLog.Printf("error indexing %s: %s", args.String(), err)
			}

			switch state {
			case indexStateSuccess:
				var branches []string
				for _, b := range args.Branches {
					branches = append(branches, fmt.Sprintf("%s=%s", b.Name, b.Version))
				}
				s.logger.Info("updated index",
					sglog.Int("tenant", args.TenantID),
					sglog.String("repo", args.Name),
					sglog.Uint32("id", args.RepoID),
					sglog.Strings("branches", branches),
					sglog.Duration("duration", elapsed),
					sglog.Duration("index_delay", indexDelay),
				)
			case indexStateSuccessMeta:
				infoLog.Printf("updated meta %s in %v", args.String(), elapsed)
			}
			s.queue.SetIndexed(opts, state)
		})

		if !ran {
			// Someone else is processing the repository. We can just skip this job
			// since the repository will be added back to the queue and we will
			// converge to the correct behaviour.
			debugLog.Printf("index job for repository already running: %s", args)
			continue
		}
	}
}

// repoNameForMetric returns a normalized version of the given repository name that is
// suitable for use with Prometheus metrics.
func repoNameForMetric(repo string) string {
	// Check to see if we want to be able to capture separate indexing metrics for this repository.
	// If we don't, set to a default string to keep the cardinality for the Prometheus metric manageable.
	if _, ok := reposWithSeparateIndexingMetrics[repo]; ok {
		return repo
	}

	return ""
}

func batched(slice []uint32, size int) <-chan []uint32 {
	c := make(chan []uint32)
	go func() {
		for len(slice) > 0 {
			if size > len(slice) {
				size = len(slice)
			}
			c <- slice[:size]
			slice = slice[size:]
		}
		close(c)
	}()
	return c
}

// jitterTicker returns a ticker which ticks with a jitter. Each tick is
// uniformly selected from the range (d/2, d + d/2). It will tick on creation.
//
// sig is a list of signals which also cause the ticker to fire. This is a
// convenience to allow manually triggering of the ticker.
func jitterTicker(d time.Duration, sig ...os.Signal) <-chan struct{} {
	ticker := make(chan struct{})

	go func() {
		for {
			ticker <- struct{}{}
			ns := int64(d)
			jitter := rand.Int63n(ns)
			time.Sleep(time.Duration(ns/2 + jitter))
		}
	}()

	go func() {
		if len(sig) == 0 {
			return
		}

		c := make(chan os.Signal, 1)
		signal.Notify(c, sig...)
		for range c {
			ticker <- struct{}{}
		}
	}()

	return ticker
}

// Index starts an index job for repo name at commit.
func (s *Server) index(ctx context.Context, args *indexArgs) (state indexState, err error) {
	tr := trace.New("index", args.Name)
	tr.SetMaxEvents(30) // Ensure we capture all indexing events

	defer func() {
		if err != nil {
			tr.SetError()
			tr.LazyPrintf("error: %v", err)
			state = indexStateFail
			metricFailingTotal.Inc()
		}
		tr.LazyPrintf("state: %s", state)
		tr.Finish()
	}()

	// Sourcegraph should always provide a tenant ID.
	if args.TenantID < 1 {
		return indexStateFail, tenant.ErrMissingTenant
	}

	tr.LazyPrintf("branches: %v", args.Branches)

	if len(args.Branches) == 0 {
		return indexStateEmpty, createEmptyShard(args)
	}

	repositoryName := args.Name
	if _, ok := s.deltaBuildRepositoriesAllowList[repositoryName]; ok {
		tr.LazyPrintf("marking this repository for delta build")
		args.UseDelta = true
	}

	args.DeltaShardNumberFallbackThreshold = s.deltaShardNumberFallbackThreshold

	if _, ok := s.repositoriesSkipSymbolsCalculationAllowList[repositoryName]; ok {
		tr.LazyPrintf("skipping symbols calculation")
		args.Symbols = false
	}

	reason := "forced"

	if args.Incremental {
		bo := args.BuildOptions()
		bo.SetDefaults()
		incrementalState, fn := bo.IndexState()
		reason = string(incrementalState)
		metricIndexIncrementalIndexState.WithLabelValues(string(incrementalState)).Inc()

		switch incrementalState {
		case index.IndexStateEqual:
			debugLog.Printf("%s index already up to date. Shard=%s", args.String(), fn)
			return indexStateNoop, nil

		case index.IndexStateMeta:
			infoLog.Printf("updating index.meta %s", args.String())

			// TODO(stefan) handle mergeMeta for tenant id.
			if err := mergeMeta(bo); err != nil {
				errorLog.Printf("falling back to full update: failed to update index.meta %s: %s", args.String(), err)
			} else {
				return indexStateSuccessMeta, nil
			}

		case index.IndexStateCorrupt:
			infoLog.Printf("falling back to full update: corrupt index: %s", args.String())
		}
	}

	infoLog.Printf("updating index %s reason=%s", args.String(), reason)

	metricIndexingTotal.Inc()
	c := gitIndexConfig{
		runCmd: func(cmd *exec.Cmd) error {
			return s.loggedRun(tr, cmd)
		},

		findRepositoryMetadata: func(args *indexArgs) (repository *zoekt.Repository, metadata *zoekt.IndexMetadata, ok bool, err error) {
			return args.BuildOptions().FindRepositoryMetadata()
		},
		timeout: s.timeout,
	}

	err = gitIndex(ctx, c, args, s.Sourcegraph, s.logger)
	if err != nil {
		return indexStateFail, err
	}

	if err := updateIndexStatusOnSourcegraph(c, args, s.Sourcegraph); err != nil {
		s.logger.Error("failed to update index status",
			sglog.String("repo", args.Name),
			sglog.Uint32("id", args.RepoID),
			sglogBranches("branches", args.Branches),
			sglog.Error(err),
		)
	}

	return indexStateSuccess, nil
}

// updateIndexStatusOnSourcegraph pushes the current state to sourcegraph so
// it can update the zoekt_repos table.
func updateIndexStatusOnSourcegraph(c gitIndexConfig, args *indexArgs, sg Sourcegraph) error {
	// We need to read from disk for IndexTime.
	_, metadata, ok, err := c.findRepositoryMetadata(args)
	if err != nil {
		return fmt.Errorf("failed to read metadata for new/updated index: %w", err)
	}
	if !ok {
		return errors.New("failed to find metadata for new/updated index")
	}

	status := []indexStatus{{
		RepoID:        args.RepoID,
		Branches:      args.Branches,
		IndexTimeUnix: metadata.IndexTime.Unix(),
	}}
	if err := sg.UpdateIndexStatus(status); err != nil {
		return fmt.Errorf("failed to update sourcegraph with status: %w", err)
	}

	return nil
}

func sglogBranches(key string, branches []zoekt.RepositoryBranch) sglog.Field {
	ss := make([]string, len(branches))
	for i, b := range branches {
		ss[i] = fmt.Sprintf("%s=%s", b.Name, b.Version)
	}
	return sglog.Strings(key, ss)
}

func (s *Server) indexArgs(opts IndexOptions) *indexArgs {
	parallelism := s.parallelism(opts, runtime.GOMAXPROCS(0))
	return &indexArgs{
		IndexOptions: opts,
		IndexDir:     s.IndexDir,
		Parallelism:  parallelism,
		Incremental:  true,
		FileLimit:    MaxFileSize,
		ShardMerging: s.shardMerging,
	}
}

// parallelism consults both the server flags and index options to determine the number
// of shards to index in parallel. If the CPUCount index option is provided, it always
// overrides the server flag.
func (s *Server) parallelism(opts IndexOptions, maxProcs int) int {
	var parallelism int
	if opts.ShardConcurrency > 0 {
		parallelism = int(opts.ShardConcurrency)
	} else {
		parallelism = s.CPUCount
	}

	// In case this was accidentally misconfigured, we cap the threads at 4 times the available CPUs
	if parallelism > 4*maxProcs {
		parallelism = 4 * maxProcs
	}

	// If index concurrency is set, then divide the parallelism by the number of
	// repos we're indexing in parallel
	if s.IndexConcurrency > 1 {
		parallelism = int(math.Ceil(float64(parallelism) / float64(s.IndexConcurrency)))
	}

	return parallelism
}

func createEmptyShard(args *indexArgs) error {
	bo := args.BuildOptions()
	bo.SetDefaults()
	bo.RepositoryDescription.Branches = []zoekt.RepositoryBranch{{Name: "HEAD", Version: "404aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}

	if args.Incremental && bo.IncrementalSkipIndexing() {
		return nil
	}

	builder, err := index.NewBuilder(*bo)
	if err != nil {
		return err
	}
	return builder.Finish()
}

// addDebugHandlers adds handlers specific to indexserver.
func (s *Server) addDebugHandlers(mux *http.ServeMux) {
	// Sourcegraph's site admin view requires indexserver to serve it's admin view
	// on "/".
	mux.Handle("/", http.HandlerFunc(s.handleRoot))

	mux.Handle("/debug/reindex", http.HandlerFunc(s.handleReindex))
	mux.Handle("/debug/indexed", http.HandlerFunc(s.handleDebugIndexed))
	mux.Handle("/debug/list", http.HandlerFunc(s.handleDebugList))
	mux.Handle("/debug/merge", http.HandlerFunc(s.handleDebugMerge))
	mux.Handle("/debug/queue", http.HandlerFunc(s.queue.handleDebugQueue))
	mux.Handle("/debug/host", http.HandlerFunc(s.handleHost))
}

func (s *Server) handleHost(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	response := struct {
		Hostname string
	}{
		Hostname: s.hostname,
	}

	b, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(b)
}

var rootTmpl = template.Must(template.New("name").Parse(`
<html>
    <body>
        <a href="debug">Debug</a><br />
        <a href="debug/requests">Traces</a><br />
        {{.IndexMsg}}<br />
        <br />
        <h3>Reindex</h3>
        {{if .Repos}}
            <a href="?show_repos=false">hide repos</a><br />
            <table style="margin-top: 20px">
                <th style="text-align:left">Name</th>
                <th style="text-align:left">ID (click to reindex)</th>
                {{range .Repos}}
                    <tr>
                        <td>{{.Name}}</td>
                        <td><a href="?id={{.ID}}&show_repos=true">{{.ID}}</a></id>
                    </tr>
                {{end}}
            </table>
        {{else}}
            <a href="?show_repos=true">show repos</a><br />
        {{end}}
    </body>
</html>
`))

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	values := r.URL.Query()

	// ?id=
	indexMsg := ""
	if v := values.Get("id"); v != "" {
		id, err := strconv.Atoi(v)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		indexMsg, _ = s.forceIndex(r.Context(), uint32(id))
	}

	// ?show_repos=
	showRepos := false
	if v := values.Get("show_repos"); v != "" {
		showRepos, _ = strconv.ParseBool(v)
	}

	type Repo struct {
		ID   uint32
		Name string
	}
	var data struct {
		Repos    []Repo
		IndexMsg string
	}

	data.IndexMsg = indexMsg

	if showRepos {
		s.queue.Iterate(func(opts *IndexOptions) {
			data.Repos = append(data.Repos, Repo{
				ID:   opts.RepoID,
				Name: opts.Name,
			})
		})
		sort.Slice(data.Repos, func(i, j int) bool { return data.Repos[i].Name < data.Repos[j].Name })
	}

	_ = rootTmpl.Execute(w, data)
}

// handleReindex triggers a reindex asynocronously. If a reindex was triggered
// the request returns with status 202. The caller can infer the new state of
// the index by calling List.
func (s *Server) handleReindex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id, err := strconv.Atoi(r.Form.Get("repo"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	go func() { s.forceIndex(r.Context(), uint32(id)) }()

	// 202 Accepted
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleDebugList(w http.ResponseWriter, r *http.Request) {
	withIndexed := true
	if b, err := strconv.ParseBool(r.URL.Query().Get("indexed")); err == nil {
		withIndexed = b
	}

	var indexed []uint32
	if withIndexed {
		indexed = listIndexed(s.IndexDir)
	}

	repos, err := s.Sourcegraph.List(r.Context(), indexed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	bw := bytes.Buffer{}

	tw := tabwriter.NewWriter(&bw, 16, 8, 4, ' ', 0)

	_, err = fmt.Fprintf(tw, "ID\tName\n")
	if err != nil {
		http.Error(w, fmt.Sprintf("writing column headers: %s", err), http.StatusInternalServerError)
		return
	}

	s.queue.mu.Lock()
	name := ""
	for _, id := range repos.IDs {
		if item := s.queue.get(id); item != nil {
			name = item.opts.Name
		} else {
			name = ""
		}
		_, err = fmt.Fprintf(tw, "%d\t%s\n", id, name)
		if err != nil {
			debugLog.Printf("handleDebugList: %s\n", err.Error())
		}
	}
	s.queue.mu.Unlock()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = tw.Flush()
	if err != nil {
		http.Error(w, fmt.Sprintf("flushing tabwriter: %s", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(bw.Len()))

	if _, err := io.Copy(w, &bw); err != nil {
		http.Error(w, fmt.Sprintf("copying output to response writer: %s", err), http.StatusInternalServerError)
		return
	}
}

// handleDebugMerge triggers a merge even if shard merging is not enabled. Users
// can run this command during periods of low usage (evenings, weekends) to
// trigger an initial merge run. In the steady-state, merges happen rarely, even
// on busy instances, and users can rely on automatic merging instead.
func (s *Server) handleDebugMerge(w http.ResponseWriter, _ *http.Request) {
	// A merge operation can take very long, depending on the number merges and the
	// target size of the compound shards. We run the merge in the background and
	// return immediately to the user.
	//
	// We track the status of the merge with metricShardMergingRunning.
	go func() {
		s.doMerge()
	}()
	_, _ = w.Write([]byte("merging enqueued\n"))
}

func (s *Server) handleDebugIndexed(w http.ResponseWriter, r *http.Request) {
	indexed := listIndexed(s.IndexDir)

	bw := bytes.Buffer{}

	tw := tabwriter.NewWriter(&bw, 16, 8, 4, ' ', 0)

	_, err := fmt.Fprintf(tw, "ID\tName\n")
	if err != nil {
		http.Error(w, fmt.Sprintf("writing column headers: %s", err), http.StatusInternalServerError)
		return
	}

	s.queue.mu.Lock()
	name := ""
	for _, id := range indexed {
		if item := s.queue.get(id); item != nil {
			name = item.opts.Name
		} else {
			name = ""
		}
		_, err = fmt.Fprintf(tw, "%d\t%s\n", id, name)
		if err != nil {
			debugLog.Printf("handleDebugIndexed: %s\n", err.Error())
		}
	}
	s.queue.mu.Unlock()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = tw.Flush()
	if err != nil {
		http.Error(w, fmt.Sprintf("flushing tabwriter: %s", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(bw.Len()))

	if _, err := io.Copy(w, &bw); err != nil {
		http.Error(w, fmt.Sprintf("copying output to response writer: %s", err), http.StatusInternalServerError)
		return
	}
}

// forceIndex will run the index job for repo name now. It will return always
// return a string explaining what it did, even if it failed.
func (s *Server) forceIndex(ctx context.Context, id uint32) (string, error) {
	var opts IndexOptions
	var err error
	s.Sourcegraph.ForceIterateIndexOptions(func(o IndexOptions) {
		opts = o
	}, func(_ uint32, e error) {
		err = e
	}, id)
	if err != nil {
		return fmt.Sprintf("Indexing %d failed: %v", id, err), err
	}

	args := s.indexArgs(opts)
	args.Incremental = false // force re-index

	var state indexState
	ran := s.muIndexDir.With(opts.Name, func() {
		state, err = s.index(ctx, args)
	})
	if !ran {
		return fmt.Sprintf("index job for repository already running: %s", args), nil
	}
	if err != nil {
		return fmt.Sprintf("Indexing %s failed: %s", args.String(), err), err
	}
	return fmt.Sprintf("Indexed %s with state %s", args.String(), state), nil
}

// Index implements the gRPC method for indexing a repository. If the repository
// exists in the trash, it will attempt to recover it before indexing. The
// method returns metadata about the indexed repository including branches and
// and the time of indexing.  The metadata might be empty if we fail to read the
// metadata after the index or if the repository is empty.
//
// Possible GRPC error codes:
// - InvalidArgument: returned when the index options are missing or invalid
// - AlreadyExists: returned when the repository is already being indexed by another process
// - Internal: returned when indexing fails
// - Canceled: returned when the context is canceled while waiting for an index slot
func (s *Server) Index(ctx context.Context, req *indexserverv1.IndexRequest) (*indexserverv1.IndexResponse, error) {
	return s.indexGRPC(ctx, req, s.index)
}

// indexGRPC takes an index func which is used for testing. For production GRPC calls, use
// Server.Index instead
func (s *Server) indexGRPC(ctx context.Context, req *indexserverv1.IndexRequest, indexFunc func(ctx context.Context, args *indexArgs) (indexState, error)) (*indexserverv1.IndexResponse, error) {
	// Validate request
	if req.Options == nil {
		return nil, status.Error(codes.InvalidArgument, "index options are required")
	}

	// Try to acquire semaphore, but respect context cancellation
	select {
	case s.indexSemaphore <- struct{}{}:
		defer func() { <-s.indexSemaphore }()
	case <-ctx.Done():
		return nil, status.Error(codes.Canceled, "context canceled while waiting for index slot")
	case <-time.After(s.timeout):
		return nil, status.Error(codes.DeadlineExceeded, "timed out while waiting for index slot")
	}

	var opts IndexOptions
	opts.FromProto(req.Options)
	opts.CloneURL = s.rootURL.ResolveReference(&url.URL{Path: path.Join("/.internal/git", req.Options.Name)}).String()
	args := s.indexArgs(opts)

	// Recover the index from the trash if possible. First we have to make sure
	// the shard doesn't exist in the index dir, because it might potentially be
	// overwritten if we recover a shard from the trash. If we can find a shard
	// in the index dir, we skip recovery. In both cases we continue and let the
	// index process figure out whether the shard is up-to-date or requires a
	// reindex.
	if _, _, ok, err := args.BuildOptions().FindRepositoryMetadata(); err == nil && !ok {
		if s.recoverFromTrash(opts.RepoID) {
			infoLog.Printf("restored repository %d from trash", opts.RepoID)
		}
	}

	// Index the repository.
	var indexErr error
	var indexTimeUnix int64
	var zoektRepo *zoekt.Repository

	ran := s.muIndexDir.With(opts.Name, func() {
		// only record time taken once we hold the lock. This avoids us
		// recording time taken while merging/cleanup runs.
		start := time.Now()

		var state indexState
		state, indexErr = indexFunc(ctx, args)
		elapsed := time.Since(start)
		metricIndexDuration.WithLabelValues(string(state), repoNameForMetric(opts.Name)).Observe(elapsed.Seconds())

		if indexErr != nil {
			errorLog.Printf("error indexing %s: %s", args.String(), indexErr)
			indexErr = status.Errorf(codes.Internal, "failed to index repository: %v", indexErr)
			return
		}

		readMetadata := func(args *indexArgs) (*zoekt.Repository, int64, error) {
			repo, metadata, ok, err := args.BuildOptions().FindRepositoryMetadata()
			if err != nil || !ok {
				return nil, 0, fmt.Errorf("failed to read metadata for %s: %w", args.String(), err)
			}
			return repo, metadata.IndexTime.Unix(), nil
		}

		switch state {
		case indexStateSuccess:
			zoektRepo, indexTimeUnix, indexErr = readMetadata(args)
			if indexErr != nil {
				return
			}
			var branches []string
			for _, b := range zoektRepo.Branches {
				branches = append(branches, fmt.Sprintf("%s=%s", b.Name, b.Version))
			}
			s.logger.Info("updated index",
				sglog.Int("tenant", args.TenantID),
				sglog.String("repo", args.Name),
				sglog.Uint32("id", args.RepoID),
				sglog.Strings("branches", branches),
				sglog.Duration("duration", elapsed),
			)
		case indexStateSuccessMeta:
			zoektRepo, indexTimeUnix, indexErr = readMetadata(args)
			if indexErr != nil {
				return
			}
			infoLog.Printf("updated meta %s in %v", args.String(), elapsed)
		case indexStateNoop:
			zoektRepo, indexTimeUnix, indexErr = readMetadata(args)
			if indexErr != nil {
				return
			}
		case indexStateEmpty:
			zoektRepo = &zoekt.Repository{
				ID: args.RepoID,
			}
		case indexStateFail:
			// This should never happen, because indexStateFail implies
			// indexErr!=nil and we exit early, but we'll handle it gracefully
			// just in case.
			indexErr = status.Error(codes.Internal, "failed to index repository")
			// Repository exists but is empty, return OK with empty response
		default:
			indexErr = status.Errorf(codes.Internal, "unknown index state: %s", state)
		}
	})

	if !ran {
		// Someone else is processing the repository.
		debugLog.Printf("index job for repository already running: %s", args)
		return nil, status.Error(codes.AlreadyExists, "repository is already being indexed")
	}

	if indexErr != nil {
		return nil, indexErr
	}

	if zoektRepo == nil {
		// This should never happen, because zoektRepo=nil means we either
		// failed to index or failed to read the metadata, both of which yield
		// indexErr!=nil.
		return nil, status.Error(codes.Internal, fmt.Sprintf("unexpected error: zoektRepo is nil for options %+v", opts))
	}

	// Convert branches to proto format
	repoBranchesProto := make([]*configv1.ZoektRepositoryBranch, len(zoektRepo.Branches))
	for i, b := range zoektRepo.Branches {
		repoBranchesProto[i] = &configv1.ZoektRepositoryBranch{
			Name:    b.Name,
			Version: b.Version,
		}
	}

	return &indexserverv1.IndexResponse{
		RepoId:        zoektRepo.ID,
		Branches:      repoBranchesProto,
		IndexTimeUnix: indexTimeUnix,
	}, nil
}

// recoverFromTrash attempts to recover an index from the trash or tombstones.
// It returns true if the repository was recovered.
func (s *Server) recoverFromTrash(repoID uint32) bool {
	trashShards := getShards(filepath.Join(s.IndexDir, ".trash"))
	if shards, ok := trashShards[repoID]; ok {
		moveAll(s.IndexDir, shards)
		return true
	}

	tombstones := getTombstonedRepos(s.IndexDir)
	if tombstone, ok := tombstones[repoID]; ok {
		if err := index.UnsetTombstone(tombstone.Path, repoID); err == nil {
			return true
		}
	}

	return false
}

// Delete implements the gRPC method for deleting a repository. It moves the
// simple shards to the trash dir and tombstones repos in compound shards.
func (s *Server) Delete(ctx context.Context, req *indexserverv1.DeleteRequest) (*indexserverv1.DeleteResponse, error) {
	var err error

	// Run the delete operation in a goroutine to be able to respond to context
	// cancellation while waiting for the lock.
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.muIndexDir.Global(func() {
			indexShards := getShards(s.IndexDir)
			for _, repoID := range req.RepoIds {
				if ctx.Err() != nil {
					err = status.Error(codes.Canceled, "context canceled")
					return
				}

				if shards, ok := indexShards[repoID]; ok {
					simple := shards[:0]
					for _, shardItem := range shards {
						if s.shardMerging && maybeSetTombstone([]shard{shardItem}, repoID) {
							continue
						}

						simple = append(simple, shardItem)
					}

					if len(simple) == 0 {
						continue
					}

					moveAll(filepath.Join(s.IndexDir, ".trash"), simple)
				}
			}
		})
	}()

	select {
	case <-done:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, status.Error(codes.Canceled, "context canceled")
	case <-time.After(s.timeout):
		return nil, status.Error(codes.DeadlineExceeded, "timed out while waiting for delete operation to complete")
	}

	return &indexserverv1.DeleteResponse{}, nil
}

// DeleteAllData deletes all shards in the index and trash dir belonging to the
// tenant associated with the request. The deletion is best-effort, which means
// we will delete as much as possible. If no error is returned, the caller can
// be certain that all data has been deleted.
func (s *Server) DeleteAllData(ctx context.Context, _ *indexserverv1.DeleteAllDataRequest) (*indexserverv1.DeleteAllDataResponse, error) {
	tnt, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, err
	}
	s.logger.Warn("DeleteAllData", sglog.Int("tenant_id", tnt.ID()))

	var merr error
	s.muIndexDir.Global(func() {
		// First, explode all compound shards that have repos from the tenant in
		// question. Because we hold the global lock, we can be sure that no new
		// merges start while we do this.
		if err := s.explodeTenantCompoundShards(ctx, func(path string) error {
			// We call explode in a separate process to protect indexserver.
			cmd := defaultExplodeCmd(path)

			stdoutBuf := &bytes.Buffer{}
			stderrBuf := &bytes.Buffer{}
			cmd.Stdout = stdoutBuf
			cmd.Stderr = stderrBuf

			err := cmd.Run()
			if err != nil {
				errorLog.Printf("explode failed: %v (stderr: %s)", err, stderrBuf.String())
				return err
			}

			infoLog.Printf("exploded shard: %s", stdoutBuf.String())

			return nil
		}); err != nil {
			merr = multierr.Append(merr, err)
		}

		// Invariant: all shards from the tenant are simple shards.

		if err := purgeTenantShards(ctx, s.IndexDir); err != nil {
			merr = multierr.Append(merr, err)
		}
		if err := purgeTenantShards(ctx, filepath.Join(s.IndexDir, ".trash")); err != nil {
			merr = multierr.Append(merr, err)
		}
	})

	return &indexserverv1.DeleteAllDataResponse{}, merr
}

func listIndexed(indexDir string) []uint32 {
	index := getShards(indexDir)
	metricNumIndexed.Set(float64(len(index)))
	repoIDs := make([]uint32, 0, len(index))
	for id := range index {
		repoIDs = append(repoIDs, id)
	}
	slices.Sort(repoIDs)
	return repoIDs
}

// setupTmpDir sets up a temporary directory on the same volume as the
// indexes.
//
// If main is true we will delete older temp directories left around. main is
// false when this is a debug command.
func setupTmpDir(logger sglog.Logger, main bool, index string) error {
	// change the target tmp directory depending on if it's our main daemon or a
	// debug sub command.
	dir := ".indexserver.debug.tmp"
	if main {
		dir = ".indexserver.tmp"
	}

	tmpRoot := filepath.Join(index, dir)

	if main {
		logger.Info("removing tmp dir", sglog.String("tmpRoot", tmpRoot))
		err := os.RemoveAll(tmpRoot)
		if err != nil {
			logger.Error("failed to remove tmp dir", sglog.String("tmpRoot", tmpRoot), sglog.Error(err))
		}
	}

	if err := os.MkdirAll(tmpRoot, 0o755); err != nil {
		return err
	}

	return os.Setenv("TMPDIR", tmpRoot)
}

func printMetaData(fn string) error {
	repo, indexMeta, err := index.ReadMetadataPath(fn)
	if err != nil {
		return err
	}

	err = json.NewEncoder(os.Stdout).Encode(indexMeta)
	if err != nil {
		return err
	}

	err = json.NewEncoder(os.Stdout).Encode(repo)
	if err != nil {
		return err
	}
	return nil
}

func printShardStats(fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}

	iFile, err := index.NewIndexFile(f)
	if err != nil {
		return err
	}

	return index.PrintNgramStats(iFile)
}

func srcLogLevelIsDebug() bool {
	lvl := os.Getenv(sglog.EnvLogLevel)
	return strings.EqualFold(lvl, "dbug") || strings.EqualFold(lvl, "debug")
}

func getEnvWithDefaultBool(k string, defaultVal bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return defaultVal
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Fatalf("error parsing ENV %s to int64: %s", k, err)
	}
	return b
}

func getEnvWithDefaultInt64(k string, defaultVal int64) int64 {
	v := os.Getenv(k)
	if v == "" {
		return defaultVal
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Fatalf("error parsing ENV %s to int64: %s", k, err)
	}
	return i
}

func getEnvWithDefaultInt(k string, defaultVal int) int {
	v := os.Getenv(k)
	if v == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(k)
	if err != nil {
		log.Fatalf("error parsing ENV %s to int: %s", k, err)
	}
	return i
}

func getEnvWithDefaultUint64(k string, defaultVal uint64) uint64 {
	v := os.Getenv(k)
	if v == "" {
		return defaultVal
	}
	i, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		log.Fatalf("error parsing ENV %s to uint64: %s", k, err)
	}
	return i
}

func getEnvWithDefaultFloat64(k string, defaultVal float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return defaultVal
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Fatalf("error parsing ENV %s to float64: %s", k, err)
	}
	return f
}

func getEnvWithDefaultString(k string, defaultVal string) string {
	v := os.Getenv(k)
	if v == "" {
		return defaultVal
	}
	return v
}

func getEnvWithDefaultDuration(k string, defaultVal time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return defaultVal
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("error parsing ENV %s to duration: %s", k, err)
	}
	return d
}

func getEnvWithDefaultEmptySet(k string) map[string]struct{} {
	set := map[string]struct{}{}
	for _, v := range strings.Split(os.Getenv(k), ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			set[v] = struct{}{}
		}
	}
	return set
}

func joinStringSet(set map[string]struct{}, sep string) string {
	var xs []string
	for x := range set {
		xs = append(xs, x)
	}

	return strings.Join(xs, sep)
}

func setShardsCounter(indexDir string) {
	fns, err := filepath.Glob(filepath.Join(indexDir, "*.zoekt"))
	if err != nil {
		errorLog.Printf("setShardsCounter: %s\n", err)
		return
	}
	metricNumberShards.Set(float64(len(fns)))

	compoundFns := make([]string, 0, len(fns))
	for _, fn := range fns {
		if strings.HasPrefix(filepath.Base(fn), "compound-") {
			compoundFns = append(compoundFns, fn)
		}
	}
	metricNumberCompoundShards.Set(float64(len(compoundFns)))
}

func rootCmd() *ffcli.Command {
	rootFs := flag.NewFlagSet("rootFs", flag.ExitOnError)
	conf := rootConfig{
		Main: true,
	}
	conf.registerRootFlags(rootFs)

	return &ffcli.Command{
		FlagSet:     rootFs,
		ShortUsage:  "zoekt-sourcegraph-indexserver [flags] [<subcommand>]",
		Subcommands: []*ffcli.Command{debugCmd()},
		Exec: func(ctx context.Context, args []string) error {
			return startServer(conf)
		},
	}
}

type rootConfig struct {
	// Main is true if this rootConfig is for our main long running command (the
	// indexserver). Debug commands should not set this value. This is used to
	// determine if we need to run tmpfriend.
	Main bool

	root             string
	interval         time.Duration
	index            string
	indexConcurrency int64
	listen           string
	hostname         string
	cpuFraction      float64

	// config values related to shard merging
	disableShardMerging bool
	vacuumInterval      time.Duration
	mergeInterval       time.Duration
	targetSize          int64
	minSize             int64
	minAgeDays          int

	// config values related to backoff indexing repos with one or more consecutive failures
	backoffDuration    time.Duration
	maxBackoffDuration time.Duration
}

func (rc *rootConfig) registerRootFlags(fs *flag.FlagSet) {
	fs.StringVar(&rc.root, "sourcegraph_url", os.Getenv("SRC_FRONTEND_INTERNAL"), "http://sourcegraph-frontend-internal or http://localhost:3090. If a path to a directory, we fake the Sourcegraph API and index all repos rooted under path.")
	fs.DurationVar(&rc.interval, "interval", time.Minute, "sync with sourcegraph this often")
	fs.Int64Var(&rc.indexConcurrency, "index_concurrency", getEnvWithDefaultInt64("SRC_INDEX_CONCURRENCY", 1), "the number of repos to index concurrently")
	fs.StringVar(&rc.index, "index", getEnvWithDefaultString("DATA_DIR", index.DefaultDir), "set index directory to use")
	fs.StringVar(&rc.listen, "listen", ":6072", "listen on this address.")
	fs.StringVar(&rc.hostname, "hostname", index.HostnameBestEffort(), "the name we advertise to Sourcegraph when asking for the list of repositories to index. Can also be set via the NODE_NAME environment variable.")
	fs.Float64Var(&rc.cpuFraction, "cpu_fraction", 1.0, "use this fraction of the cores for indexing.")
	fs.DurationVar(&rc.backoffDuration, "backoff_duration", getEnvWithDefaultDuration("BACKOFF_DURATION", 10*time.Minute), "for the given duration we backoff from enqueue operations for a repository that's failed its previous indexing attempt. Consecutive failures increase the duration of the delay linearly up to the maxBackoffDuration. A negative value disables indexing backoff.")
	fs.DurationVar(&rc.maxBackoffDuration, "max_backoff_duration", getEnvWithDefaultDuration("MAX_BACKOFF_DURATION", 120*time.Minute), "the maximum duration to backoff from enqueueing a repo for indexing.  A negative value disables indexing backoff.")

	// flags related to shard merging
	fs.BoolVar(&rc.disableShardMerging, "shard_merging", getEnvWithDefaultBool("SRC_DISABLE_SHARD_MERGING", false), "disable shard merging")
	fs.DurationVar(&rc.vacuumInterval, "vacuum_interval", getEnvWithDefaultDuration("SRC_VACUUM_INTERVAL", 24*time.Hour), "run vacuum this often")
	fs.DurationVar(&rc.mergeInterval, "merge_interval", getEnvWithDefaultDuration("SRC_MERGE_INTERVAL", 8*time.Hour), "run merge this often")
	fs.Int64Var(&rc.targetSize, "merge_target_size", getEnvWithDefaultInt64("SRC_MERGE_TARGET_SIZE", 1000), "the target size of compound shards in MiB")
	fs.Int64Var(&rc.minSize, "merge_min_size", getEnvWithDefaultInt64("SRC_MERGE_MIN_SIZE", 800), "the minimum size of a compound shard in MiB")
	fs.IntVar(&rc.minAgeDays, "merge_min_age", getEnvWithDefaultInt("SRC_MERGE_MIN_AGE", 7), "the time since the last commit in days. Shards with newer commits are excluded from merging.")
}

func startServer(conf rootConfig) error {
	s, err := newServer(conf)
	if err != nil {
		return err
	}

	profiler.Init("zoekt-sourcegraph-indexserver")
	setShardsCounter(s.IndexDir)

	if conf.listen != "" {

		mux := http.NewServeMux()
		debugserver.AddHandlers(mux, true, []debugserver.DebugPage{
			{Href: "debug/indexed", Text: "Indexed", Description: "list of all indexed repositories"},
			{Href: "debug/list?indexed=false", Text: "Assigned (this instance)", Description: "list of all repositories that are assigned to this instance"},
			{Href: "debug/list?indexed=true", Text: "Assigned (all)", Description: "same as above, but includes repositories which this instance temporarily holds during re-balancing"},
			{Href: "debug/queue", Text: "Indexing Queue State", Description: "list of all repositories in the indexing queue, sorted by descending priority"},
		}...)
		s.addDebugHandlers(mux)

		go func() {
			debugLog.Printf("serving HTTP on %s", conf.listen)
			mux := grpcutil.MultiplexGRPC(newGRPCServer(sglog.Scoped("indexserver"), s), mux)
			log.Fatal(http.ListenAndServe(conf.listen, mux))
		}()

		// Serve mux on a unix domain socket on a best-effort-basis so that
		// webserver can call the endpoints via the shared filesystem.
		//
		// 2022-12-08: Docker for Mac with VirtioFS enabled will fail to listen
		// on the socket due to permission errors. See
		// https://github.com/docker/for-mac/issues/6239
		go func() {
			serveHTTPOverSocket := func() error {
				socket := filepath.Join(s.IndexDir, "indexserver.sock")
				// We cannot bind a socket to an existing pathname.
				if err := os.Remove(socket); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("error removing socket file: %s", socket)
				}
				// The "unix" network corresponds to stream sockets. (cf. unixgram,
				// unixpacket).
				l, err := net.Listen("unix", socket)
				if err != nil {
					return fmt.Errorf("failed to listen on socket %s: %w", socket, err)
				}
				// Indexserver (root) and webserver (Sourcegraph) run with
				// different users. Per default, the socket is created with
				// permission 755 (root root), which doesn't let webserver write to
				// it.
				//
				// See https://github.com/golang/go/issues/11822 for more context.
				if err := os.Chmod(socket, 0o777); err != nil {
					return fmt.Errorf("failed to change permission of socket %s: %w", socket, err)
				}
				debugLog.Printf("serving HTTP on %s", socket)
				return http.Serve(l, mux)
			}
			debugLog.Print(serveHTTPOverSocket())
		}()
	}

	oc := &ownerChecker{
		Path:     filepath.Join(conf.index, "owner.txt"),
		Hostname: conf.hostname,
	}
	go oc.Run()

	logger := sglog.Scoped("metricsRegistration")
	opts := mountinfo.CollectorOpts{Namespace: "zoekt_indexserver"}

	c := mountinfo.NewCollector(logger, opts, map[string]string{"indexDir": conf.index})
	prometheus.DefaultRegisterer.MustRegister(c)

	s.Run()
	return nil
}

func newServer(conf rootConfig) (*Server, error) {
	logger := sglog.Scoped("server")

	if conf.cpuFraction <= 0.0 || conf.cpuFraction > 1.0 {
		return nil, fmt.Errorf("cpu_fraction must be between 0.0 and 1.0")
	}
	if conf.index == "" {
		return nil, fmt.Errorf("must set -index")
	}
	if conf.root == "" {
		return nil, fmt.Errorf("must set -sourcegraph_url")
	}
	rootURL, err := url.Parse(conf.root)
	if err != nil {
		return nil, fmt.Errorf("url.Parse(%v): %v", conf.root, err)
	}

	rootURL = addDefaultPort(rootURL)

	// Tune GOMAXPROCS to match Linux container CPU quota.
	_, _ = maxprocs.Set()

	// Automatically prepend our own path at the front, to minimize
	// required configuration.
	if l, err := os.Readlink("/proc/self/exe"); err == nil {
		os.Setenv("PATH", filepath.Dir(l)+":"+os.Getenv("PATH"))
	}

	if _, err := os.Stat(conf.index); err != nil {
		if err := os.MkdirAll(conf.index, 0o755); err != nil {
			return nil, fmt.Errorf("MkdirAll %s: %v", conf.index, err)
		}
	}

	if err := setupTmpDir(logger, conf.Main, conf.index); err != nil {
		return nil, fmt.Errorf("failed to setup TMPDIR under %s: %v", conf.index, err)
	}

	if srcLogLevelIsDebug() {
		debugLog.SetOutput(os.Stderr)
	}

	reposWithSeparateIndexingMetrics = getEnvWithDefaultEmptySet("INDEXING_METRICS_REPOS_ALLOWLIST")
	if len(reposWithSeparateIndexingMetrics) > 0 {
		debugLog.Printf("capturing separate indexing metrics for: %s", joinStringSet(reposWithSeparateIndexingMetrics, ", "))
	}

	deltaBuildRepositoriesAllowList := getEnvWithDefaultEmptySet("DELTA_BUILD_REPOS_ALLOWLIST")
	if len(deltaBuildRepositoriesAllowList) > 0 {
		debugLog.Printf("using delta shard builds for: %s", joinStringSet(deltaBuildRepositoriesAllowList, ", "))
	}

	deltaShardNumberFallbackThreshold := getEnvWithDefaultUint64("DELTA_SHARD_NUMBER_FALLBACK_THRESHOLD", 150)
	if deltaShardNumberFallbackThreshold > 0 {
		debugLog.Printf("setting delta shard fallback threshold to %d shard(s)", deltaShardNumberFallbackThreshold)
	} else {
		debugLog.Printf("disabling delta build fallback behavior - delta builds will be performed regardless of the number of preexisting shards")
	}

	reposShouldSkipSymbolsCalculation := getEnvWithDefaultEmptySet("SKIP_SYMBOLS_REPOS_ALLOWLIST")
	if len(reposShouldSkipSymbolsCalculation) > 0 {
		debugLog.Printf("skipping generating symbols metadata for: %s", joinStringSet(reposShouldSkipSymbolsCalculation, ", "))
	}

	indexingTimeout := getEnvWithDefaultDuration("INDEXING_TIMEOUT", defaultIndexingTimeout)
	if indexingTimeout != defaultIndexingTimeout {
		debugLog.Printf("using configured indexing timeout: %s", indexingTimeout)
	}

	var sg Sourcegraph
	if rootURL.IsAbs() {
		var batchSize int
		if v := os.Getenv("SRC_REPO_CONFIG_BATCH_SIZE"); v != "" {
			batchSize, err = strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("invalid value for SRC_REPO_CONFIG_BATCH_SIZE, must be int")
			}
		}

		opts := []SourcegraphClientOption{
			WithBatchSize(batchSize),
		}

		logger := sglog.Scoped("zoektConfigurationGRPCClient")
		client, err := dialGRPCClient(rootURL.Host, logger)
		if err != nil {
			return nil, fmt.Errorf("initializing gRPC connection to %q: %w", rootURL.Host, err)
		}

		sg = newSourcegraphClient(rootURL, conf.hostname, client, opts...)

	} else {
		sg = sourcegraphFake{
			RootDir: rootURL.String(),
			Log:     log.New(os.Stderr, "sourcegraph: ", log.LstdFlags),
		}
	}

	cpuCount := max(int(math.Round(float64(runtime.GOMAXPROCS(0))*(conf.cpuFraction))), 1)

	if conf.indexConcurrency < 1 {
		conf.indexConcurrency = 1
	} else if conf.indexConcurrency > int64(cpuCount) {
		conf.indexConcurrency = int64(cpuCount)
	}

	q := NewQueue(conf.backoffDuration, conf.maxBackoffDuration, logger)

	return &Server{
		logger:                            logger,
		rootURL:                           rootURL,
		Sourcegraph:                       sg,
		IndexDir:                          conf.index,
		IndexConcurrency:                  int(conf.indexConcurrency),
		Interval:                          conf.interval,
		CPUCount:                          cpuCount,
		queue:                             *q,
		shardMerging:                      !conf.disableShardMerging,
		deltaBuildRepositoriesAllowList:   deltaBuildRepositoriesAllowList,
		deltaShardNumberFallbackThreshold: deltaShardNumberFallbackThreshold,
		repositoriesSkipSymbolsCalculationAllowList: reposShouldSkipSymbolsCalculation,
		hostname: conf.hostname,
		mergeOpts: mergeOpts{
			vacuumInterval:  conf.vacuumInterval,
			mergeInterval:   conf.mergeInterval,
			targetSizeBytes: conf.targetSize * 1024 * 1024,
			minSizeBytes:    conf.minSize * 1024 * 1024,
			minAgeDays:      conf.minAgeDays,
		},
		timeout:        indexingTimeout,
		indexSemaphore: make(chan struct{}, int(conf.indexConcurrency)),
	}, err
}

func newGRPCServer(logger sglog.Logger, s *Server, additionalOpts ...grpc.ServerOption) *grpc.Server {
	grpcServer := defaults.NewServer(logger, additionalOpts...)
	indexserverv1.RegisterSourcegraphIndexserverServiceServer(grpcServer, s)
	return grpcServer
}

// defaultGRPCServiceConfigurationJSON is the default gRPC service configuration
// for the indexed-search-configuration gRPC service.
//
// The default backoff strategy is modeled after the default settings used by
// retryablehttp.DefaultClient.
//
// It retries on the following errors (see https://grpc.github.io/grpc/core/md_doc_statuscodes.html):
//   - Unavailable
//   - Aborted
//
//go:embed default_grpc_service_configuration.json
var defaultGRPCServiceConfigurationJSON string

func internalActorUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "X-Sourcegraph-Actor-UID", "internal")
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func internalActorStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "X-Sourcegraph-Actor-UID", "internal")
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// defaultGRPCMessageReceiveSizeBytes is the default message size that gRPCs servers and clients are allowed to process.
// This can be overridden by providing custom Server/Dial options.
const defaultGRPCMessageReceiveSizeBytes = 90 * 1024 * 1024 // 90 MB

func dialGRPCClient(addr string, logger sglog.Logger, additionalOpts ...grpc.DialOption) (configv1.ZoektConfigurationServiceClient, error) {
	metrics := clientMetricsOnce()

	// If the service seems to be unavailable, this
	// will retry after [1s, 2s, 4s, 8s, 16s] with a jitterFraction of .1
	// Ex: (on the first retry attempt, we will wait between [.9s and 1.1s])
	retryOpts := []retry.CallOption{
		retry.WithMax(5),
		retry.WithBackoff(retry.BackoffExponentialWithJitter(1*time.Second, .1)),
		retry.WithCodes(codes.Unavailable),
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainStreamInterceptor(
			metrics.StreamClientInterceptor(),
			messagesize.StreamClientInterceptor,
			internalActorStreamInterceptor(),
			internalerrs.LoggingStreamClientInterceptor(logger),
			internalerrs.PrometheusStreamClientInterceptor,
			retry.StreamClientInterceptor(retryOpts...),
		),
		grpc.WithChainUnaryInterceptor(
			metrics.UnaryClientInterceptor(),
			messagesize.UnaryClientInterceptor,
			internalActorUnaryInterceptor(),
			internalerrs.LoggingUnaryClientInterceptor(logger),
			internalerrs.PrometheusUnaryClientInterceptor,
			retry.UnaryClientInterceptor(retryOpts...),
		),
		grpc.WithDefaultServiceConfig(defaultGRPCServiceConfigurationJSON),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaultGRPCMessageReceiveSizeBytes)),
	}

	opts = append(opts, additionalOpts...)

	// Ensure that the message size options are set last, so they override any other
	// client-specific options that tweak the message size.
	//
	// The message size options are only provided if the environment variable is set. These options serve as an escape hatch, so they
	// take precedence over everything else with a uniform size setting that's easy to reason about.
	opts = append(opts, messagesize.MustGetClientMessageSizeFromEnv()...)

	// This dialer is used to connect via gRPC to the Sourcegraph instance.
	// This is done lazily, so we can provide the client to use regardless of
	// whether we enabled gRPC or not initially.
	cc, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dialing %q: %w", addr, err)
	}

	client := configv1.NewZoektConfigurationServiceClient(cc)
	return client, nil
}

// addDefaultPort adds a default port to a URL if one is not specified.
//
// If the URL scheme is "http" and no port is specified, "80" is used.
// If the scheme is "https", "443" is used.
//
// The original URL is not mutated. A copy is modified and returned.
func addDefaultPort(original *url.URL) *url.URL {
	if original == nil {
		return nil // don't panic
	}

	if !original.IsAbs() {
		return original // don't do anything if the URL doesn't look like a remote URL
	}

	if original.Scheme == "http" && original.Port() == "" {
		u := cloneURL(original)
		u.Host = net.JoinHostPort(u.Host, "80")
		return u
	}

	if original.Scheme == "https" && original.Port() == "" {
		u := cloneURL(original)
		u.Host = net.JoinHostPort(u.Host, "443")
		return u
	}

	return original
}

// cloneURL returns a copy of the URL. It is safe to mutate the returned URL.
// This is copied from net/http/clone.go
func cloneURL(u *url.URL) *url.URL {
	if u == nil {
		return nil
	}
	u2 := new(url.URL)
	*u2 = *u
	if u.User != nil {
		u2.User = new(url.Userinfo)
		*u2.User = *u.User
	}
	return u2
}

func main() {
	liblog := sglog.Init(sglog.Resource{
		Name:       "zoekt-indexserver",
		Version:    index.Version,
		InstanceID: index.HostnameBestEffort(),
	})
	defer liblog.Sync()

	if err := rootCmd().ParseAndRun(context.Background(), os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

// getBoolFromEnvironmentVariables returns the boolean defined by the first environment
// variable listed in envVarNames that is set in the current process environment, or the defaultBool if none are set.
//
// An error is returned of the provided environment variables fails to parse as a boolean.
func getBoolFromEnvironmentVariables(envVarNames []string, defaultBool bool) (bool, error) {
	for _, envVar := range envVarNames {
		v := os.Getenv(envVar)
		if v == "" {
			continue
		}

		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("parsing environment variable %q to boolean: %v", envVar, err)
		}

		return b, nil
	}

	return defaultBool, nil
}
