package streamaggr

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envtemplate"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs/fscore"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompbmarshal"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promrelabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promutils"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/metrics"
	"github.com/cespare/xxhash/v2"
	"gopkg.in/yaml.v2"
)

var supportedOutputs = []string{
	"rate_sum",
	"rate_avg",
	"total",
	"total_prometheus",
	"increase",
	"increase_prometheus",
	"count_series",
	"count_samples",
	"unique_samples",
	"sum_samples",
	"last",
	"min",
	"max",
	"avg",
	"stddev",
	"stdvar",
	"histogram_bucket",
	"quantiles(phi1, ..., phiN)",
}

var (
	// lc contains information about all compressed labels for streaming aggregation
	lc promutils.LabelsCompressor

	_ = metrics.NewGauge(`vm_streamaggr_labels_compressor_size_bytes`, func() float64 {
		return float64(lc.SizeBytes())
	})

	_ = metrics.NewGauge(`vm_streamaggr_labels_compressor_items_count`, func() float64 {
		return float64(lc.ItemsCount())
	})
)

// LoadFromFile loads Aggregators from the given path and uses the given pushFunc for pushing the aggregated data.
//
// opts can contain additional options. If opts is nil, then default options are used.
//
// The returned Aggregators must be stopped with MustStop() when no longer needed.
func LoadFromFile(path string, pushFunc PushFunc, opts *Options) (*Aggregators, error) {
	a := &Aggregators{
		path:     path,
		pushFunc: pushFunc,
		opts:     opts,
	}
	if err := a.load(); err != nil {
		return nil, fmt.Errorf("cannot initialize aggregators from %q: %w; see https://docs.victoriametrics.com/stream-aggregation/#stream-aggregation-config", a.path, err)
	}
	return a, nil
}

// Reload reads config file and updates aggregators if there're any changes
func (a *Aggregators) Reload() error {
	if a == nil {
		return nil
	}
	metrics.GetOrCreateCounter(fmt.Sprintf(`vm_streamaggr_config_reloads_total{path=%q}`, a.path)).Inc()
	if err := a.load(); err != nil {
		metrics.GetOrCreateCounter(fmt.Sprintf(`vm_streamaggr_config_reloads_errors_total{path=%q}`, a.path)).Inc()
		metrics.GetOrCreateCounter(fmt.Sprintf(`vm_streamaggr_config_reload_successful{path=%q}`, a.path)).Set(0)
		return fmt.Errorf("cannot load stream aggregation config %q: %w", a.path, err)
	}
	metrics.GetOrCreateCounter(fmt.Sprintf(`vm_streamaggr_config_reload_successful{path=%q}`, a.path)).Set(1)
	metrics.GetOrCreateCounter(fmt.Sprintf(`vm_streamaggr_config_reload_success_timestamp_seconds{path=%q}`, a.path)).Set(fasttime.UnixTimestamp())
	return nil
}

// Options contains optional settings for the Aggregators.
type Options struct {
	// DedupInterval is deduplication interval for samples received for the same time series.
	//
	// The last sample per each series is left per each DedupInterval if DedupInterval > 0.
	//
	// By default deduplication is disabled.
	//
	// The deduplication can be set up individually per each aggregation via dedup_interval option.
	DedupInterval time.Duration

	// DropInputLabels is an optional list of labels to drop from samples before de-duplication and stream aggregation.
	DropInputLabels []string

	// NoAlignFlushToInterval disables alignment of flushes to the aggregation interval.
	//
	// By default flushes are aligned to aggregation interval.
	//
	// The alignment of flushes can be disabled individually per each aggregation via no_align_flush_to_interval option.
	NoAlignFlushToInterval bool

	// FlushOnShutdown enables flush of incomplete aggregation state on startup and shutdown.
	//
	// By default incomplete state is dropped.
	//
	// The flush of incomplete state can be enabled individually per each aggregation via flush_on_shutdown option.
	FlushOnShutdown bool

	// KeepMetricNames instructs to leave metric names as is for the output time series without adding any suffix.
	//
	// By default the following suffix is added to every output time series:
	//
	//     input_name:<interval>[_by_<by_labels>][_without_<without_labels>]_<output>
	//
	// This option can be overridden individually per each aggregation via keep_metric_names option.
	KeepMetricNames bool

	// IgnoreOldSamples instructs to ignore samples with timestamps older than the current aggregation interval.
	//
	// By default all the samples are taken into account.
	//
	// This option can be overridden individually per each aggregation via ignore_old_samples option.
	IgnoreOldSamples bool

	// IgnoreFirstIntervals sets amount of aggregation intervals to ignore on start.
	//
	// By default, no intervals will be ignored.
	//
	// This option can be overridden individually per each aggregation via ignore_first_intervals option.
	IgnoreFirstIntervals int
}

// Config is a configuration for a single stream aggregation.
type Config struct {
	// Match is a label selector for filtering time series for the given selector.
	//
	// If the match isn't set, then all the input time series are processed.
	Match *promrelabel.IfExpression `yaml:"match,omitempty"`

	// Interval is the interval between aggregations.
	Interval string `yaml:"interval"`

	// NoAlighFlushToInterval disables aligning of flushes to multiples of Interval.
	// By default flushes are aligned to Interval.
	//
	// See also FlushOnShutdown.
	NoAlignFlushToInterval *bool `yaml:"no_align_flush_to_interval,omitempty"`

	// FlushOnShutdown defines whether to flush incomplete aggregation state on startup and shutdown.
	// By default incomplete aggregation state is dropped, since it may confuse users.
	FlushOnShutdown *bool `yaml:"flush_on_shutdown,omitempty"`

	// DedupInterval is an optional interval for deduplication.
	DedupInterval string `yaml:"dedup_interval,omitempty"`

	// Staleness interval is interval after which the series state will be reset if no samples have been sent during it.
	// The parameter is only relevant for outputs: total, total_prometheus, increase, increase_prometheus and histogram_bucket.
	StalenessInterval string `yaml:"staleness_interval,omitempty"`

	// Outputs is a list of output aggregate functions to produce.
	//
	// The following names are allowed:
	//
	// - rate_sum - calculates sum of rate for input counters
	// - rate_avg - calculates average of rate for input counters
	// - total - aggregates input counters
	// - total_prometheus - aggregates input counters, ignoring the first sample in new time series
	// - increase - calculates the increase over input series
	// - increase_prometheus - calculates the increase over input series, ignoring the first sample in new time series
	// - count_series - counts the number of unique input series
	// - count_samples - counts the input samples
	// - unique_samples - counts the number of unique sample values
	// - sum_samples - sums the input sample values
	// - last - the last biggest sample value
	// - min - the minimum sample value
	// - max - the maximum sample value
	// - avg - the average value across all the samples
	// - stddev - standard deviation across all the samples
	// - stdvar - standard variance across all the samples
	// - histogram_bucket - creates VictoriaMetrics histogram for input samples
	// - quantiles(phi1, ..., phiN) - quantiles' estimation for phi in the range [0..1]
	//
	// The output time series will have the following names by default:
	//
	//   input_name:<interval>[_by_<by_labels>][_without_<without_labels>]_<output>
	//
	// See also KeepMetricNames
	//
	Outputs []string `yaml:"outputs"`

	// KeepMetricNames instructs to leave metric names as is for the output time series without adding any suffix.
	KeepMetricNames *bool `yaml:"keep_metric_names,omitempty"`

	// IgnoreOldSamples instructs to ignore samples with old timestamps outside the current aggregation interval.
	IgnoreOldSamples *bool `yaml:"ignore_old_samples,omitempty"`

	// IgnoreFirstIntervals sets number of aggregation intervals to be ignored on start.
	IgnoreFirstIntervals *int `yaml:"ignore_first_intervals,omitempty"`

	// By is an optional list of labels for grouping input series.
	//
	// See also Without.
	//
	// If neither By nor Without are set, then the Outputs are calculated
	// individually per each input time series.
	By []string `yaml:"by,omitempty"`

	// Without is an optional list of labels, which must be excluded when grouping input series.
	//
	// See also By.
	//
	// If neither By nor Without are set, then the Outputs are calculated
	// individually per each input time series.
	Without []string `yaml:"without,omitempty"`

	// DropInputLabels is an optional list with labels, which must be dropped before further processing of input samples.
	//
	// Labels are dropped before de-duplication and aggregation.
	DropInputLabels *[]string `yaml:"drop_input_labels,omitempty"`

	// InputRelabelConfigs is an optional relabeling rules, which are applied on the input
	// before aggregation.
	InputRelabelConfigs []promrelabel.RelabelConfig `yaml:"input_relabel_configs,omitempty"`

	// OutputRelabelConfigs is an optional relabeling rules, which are applied
	// on the aggregated output before being sent to remote storage.
	OutputRelabelConfigs []promrelabel.RelabelConfig `yaml:"output_relabel_configs,omitempty"`
}

// Aggregators aggregates metrics passed to Push and calls pushFunc for aggregated data.
type Aggregators struct {
	as       []*aggregator
	ms       *metrics.Set
	path     string
	pushFunc PushFunc
	opts     *Options
}

func (a *Aggregators) load() error {
	data, err := fscore.ReadFileOrHTTP(a.path)
	if err != nil {
		return fmt.Errorf("cannot load aggregators: %w", err)
	}
	data, err = envtemplate.ReplaceBytes(data)
	if err != nil {
		return fmt.Errorf("cannot expand environment variables in %q: %w", a.path, err)
	}
	if err = a.loadAggregatorsFromData(data); err != nil {
		return fmt.Errorf("cannot initialize aggregators from %q: %w; see https://docs.victoriametrics.com/stream-aggregation/#stream-aggregation-config", a.path, err)
	}
	return nil
}

func (a *Aggregators) getAggregator(aggr *aggregator) *aggregator {
	if a == nil {
		return nil
	}
	if idx := slices.IndexFunc(a.as, func(ac *aggregator) bool {
		return ac.configHash == aggr.configHash
	}); idx >= 0 {
		return a.as[idx]
	}
	return nil
}

func (a *Aggregators) loadAggregatorsFromData(data []byte) error {
	var cfgs []*Config
	if err := yaml.UnmarshalStrict(data, &cfgs); err != nil {
		return fmt.Errorf("cannot parse stream aggregation config: %w", err)
	}

	ms := metrics.NewSet()
	var unchanged, ac []*aggregator
	var ignoreAggrHashes []uint64
	for i, cfg := range cfgs {
		aggr, err := newAggregator(cfg, a.pushFunc, ms, a.opts)
		if err != nil {
			// Stop already initialized aggregators before returning the error.
			for _, c := range ac[:i] {
				c.MustStop()
			}
			return fmt.Errorf("cannot initialize aggregator #%d: %w", i, err)
		}
		if oldAgg := a.getAggregator(aggr); oldAgg != nil {
			aggr.MustStop()
			unchanged = append(unchanged, oldAgg)
			ignoreAggrHashes = append(ignoreAggrHashes, oldAgg.configHash)
			continue
		}
		if slices.ContainsFunc(ac, func(x *aggregator) bool {
			return x.configHash == aggr.configHash
		}) {
			aggr.MustStop()
			continue
		}
		ac = append(ac, aggr)
	}

	metrics.GetOrCreateCounter(fmt.Sprintf(`vm_streamaggr_aggregators_stopped_total{path=%q}`, a.path)).Add(len(a.as) - len(ignoreAggrHashes))
	metrics.GetOrCreateCounter(fmt.Sprintf(`vm_streamaggr_aggregators_created_total{path=%q}`, a.path)).Add(len(ac))
	a.MustStop(ignoreAggrHashes)
	a.as = slices.Concat(unchanged, ac)

	_ = ms.NewGauge(`vm_streamaggr_dedup_state_size_bytes`, func() float64 {
		n := uint64(0)
		for _, aggr := range a.as {
			if aggr.da != nil {
				n += aggr.da.sizeBytes()
			}
		}
		return float64(n)
	})
	_ = ms.NewGauge(`vm_streamaggr_dedup_state_items_count`, func() float64 {
		n := uint64(0)
		for _, aggr := range a.as {
			if aggr.da != nil {
				n += aggr.da.itemsCount()
			}
		}
		return float64(n)
	})

	metrics.RegisterSet(ms)
	a.ms = ms
	return nil
}

// MustStop stops a.
func (a *Aggregators) MustStop(ignoreAggrHashes []uint64) {
	if a == nil {
		return
	}

	metrics.UnregisterSet(a.ms)
	a.ms = nil

	for _, aggr := range a.as {
		if ignoreAggrHashes == nil || !slices.Contains(ignoreAggrHashes, aggr.configHash) {
			aggr.MustStop()
		}
	}
	a.as = nil
}

// Equal returns true if a and b are initialized from identical configs.
func (a *Aggregators) Equal(b *Aggregators) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return slices.Compare(a.getConfigHashes(), b.getConfigHashes()) == 0
}

func (a *Aggregators) getConfigHashes() []uint64 {
	result := make([]uint64, len(a.as))
	for i := range result {
		result[i] = a.as[i].configHash
	}
	slices.Sort(result)
	return result
}

// Push pushes tss to a.
//
// Push sets matchIdxs[idx] to 1 if the corresponding tss[idx] was used in aggregations.
// Otherwise matchIdxs[idx] is set to 0.
//
// Push returns matchIdxs with len equal to len(tss).
// It re-uses the matchIdxs if it has enough capacity to hold len(tss) items.
// Otherwise it allocates new matchIdxs.
func (a *Aggregators) Push(tss []prompbmarshal.TimeSeries, matchIdxs []byte) []byte {
	matchIdxs = bytesutil.ResizeNoCopyMayOverallocate(matchIdxs, len(tss))
	for i := range matchIdxs {
		matchIdxs[i] = 0
	}
	if a == nil {
		return matchIdxs
	}

	for _, aggr := range a.as {
		aggr.Push(tss, matchIdxs)
	}

	return matchIdxs
}

// aggregator aggregates input series according to the config passed to NewAggregator
type aggregator struct {
	match *promrelabel.IfExpression

	dropInputLabels []string

	inputRelabeling  *promrelabel.ParsedConfigs
	outputRelabeling *promrelabel.ParsedConfigs

	keepMetricNames  bool
	ignoreOldSamples bool
	configHash       uint64

	by                  []string
	without             []string
	aggregateOnlyByTime bool

	// da is set to non-nil if input samples must be de-duplicated
	da *dedupAggr

	// aggrStates contains aggregate states for the given outputs
	aggrStates []aggrState

	// minTimestamp is used for ignoring old samples when ignoreOldSamples is set
	minTimestamp atomic.Int64

	// suffix contains a suffix, which should be added to aggregate metric names
	//
	// It contains the interval, labels in (by, without), plus output name.
	// For example, foo_bar metric name is transformed to foo_bar:1m_by_job
	// for `interval: 1m`, `by: [job]`
	suffix string

	wg     sync.WaitGroup
	stopCh chan struct{}

	flushDuration      *metrics.Histogram
	dedupFlushDuration *metrics.Histogram

	flushTimeouts      *metrics.Counter
	dedupFlushTimeouts *metrics.Counter
}

type aggrState interface {
	pushSamples(samples []pushSample)
	flushState(ctx *flushCtx, resetState bool)
}

// PushFunc is called by Aggregators when it needs to push its state to metrics storage
type PushFunc func(tss []prompbmarshal.TimeSeries)

// newAggregator creates new aggregator for the given cfg, which pushes the aggregate data to pushFunc.
//
// opts can contain additional options. If opts is nil, then default options are used.
//
// The returned aggregator must be stopped when no longer needed by calling MustStop().
func newAggregator(cfg *Config, pushFunc PushFunc, ms *metrics.Set, opts *Options) (*aggregator, error) {
	if opts == nil {
		opts = &Options{}
	}

	// check cfg.Interval
	if cfg.Interval == "" {
		return nil, fmt.Errorf("missing `interval` option")
	}
	interval, err := time.ParseDuration(cfg.Interval)
	if err != nil {
		return nil, fmt.Errorf("cannot parse `interval: %q`: %w", cfg.Interval, err)
	}
	if interval < time.Second {
		return nil, fmt.Errorf("aggregation interval cannot be smaller than 1s; got %s", interval)
	}

	// check cfg.DedupInterval
	dedupInterval := opts.DedupInterval
	if cfg.DedupInterval != "" {
		di, err := time.ParseDuration(cfg.DedupInterval)
		if err != nil {
			return nil, fmt.Errorf("cannot parse `dedup_interval: %q`: %w", cfg.DedupInterval, err)
		}
		dedupInterval = di
	}
	if dedupInterval > interval {
		return nil, fmt.Errorf("dedup_interval=%s cannot exceed interval=%s", dedupInterval, interval)
	}
	if dedupInterval > 0 && interval%dedupInterval != 0 {
		return nil, fmt.Errorf("interval=%s must be a multiple of dedup_interval=%s", interval, dedupInterval)
	}

	// check cfg.StalenessInterval
	stalenessInterval := interval * 2
	if cfg.StalenessInterval != "" {
		stalenessInterval, err = time.ParseDuration(cfg.StalenessInterval)
		if err != nil {
			return nil, fmt.Errorf("cannot parse `staleness_interval: %q`: %w", cfg.StalenessInterval, err)
		}
		if stalenessInterval < interval {
			return nil, fmt.Errorf("staleness_interval=%s cannot be smaller than interval=%s", cfg.StalenessInterval, cfg.Interval)
		}
	}

	// Check cfg.DropInputLabels
	dropInputLabels := opts.DropInputLabels
	if v := cfg.DropInputLabels; v != nil {
		dropInputLabels = *v
	} else {
		cfg.DropInputLabels = &dropInputLabels
	}

	// initialize input_relabel_configs and output_relabel_configs
	inputRelabeling, err := promrelabel.ParseRelabelConfigs(cfg.InputRelabelConfigs)
	if err != nil {
		return nil, fmt.Errorf("cannot parse input_relabel_configs: %w", err)
	}
	outputRelabeling, err := promrelabel.ParseRelabelConfigs(cfg.OutputRelabelConfigs)
	if err != nil {
		return nil, fmt.Errorf("cannot parse output_relabel_configs: %w", err)
	}

	// check by and without lists
	by := sortAndRemoveDuplicates(cfg.By)
	without := sortAndRemoveDuplicates(cfg.Without)
	if len(by) > 0 && len(without) > 0 {
		return nil, fmt.Errorf("`by: %s` and `without: %s` lists cannot be set simultaneously", by, without)
	}
	aggregateOnlyByTime := (len(by) == 0 && len(without) == 0)
	if !aggregateOnlyByTime && len(without) == 0 {
		by = addMissingUnderscoreName(by)
	}

	// check cfg.KeepMetricNames
	keepMetricNames := opts.KeepMetricNames
	if v := cfg.KeepMetricNames; v != nil {
		keepMetricNames = *v
	} else {
		cfg.KeepMetricNames = &keepMetricNames
	}
	if keepMetricNames {
		if len(cfg.Outputs) != 1 {
			return nil, fmt.Errorf("`ouputs` list must contain only a single entry if `keep_metric_names` is set; got %q", cfg.Outputs)
		}
		if cfg.Outputs[0] == "histogram_bucket" || strings.HasPrefix(cfg.Outputs[0], "quantiles(") && strings.Contains(cfg.Outputs[0], ",") {
			return nil, fmt.Errorf("`keep_metric_names` cannot be applied to `outputs: %q`, since they can generate multiple time series", cfg.Outputs)
		}
	}

	// check cfg.IgnoreOldSamples
	ignoreOldSamples := opts.IgnoreOldSamples
	if v := cfg.IgnoreOldSamples; v != nil {
		ignoreOldSamples = *v
	} else {
		cfg.IgnoreOldSamples = &ignoreOldSamples
	}

	// check cfg.IgnoreFirstIntervals
	ignoreFirstIntervals := opts.IgnoreFirstIntervals
	if v := cfg.IgnoreFirstIntervals; v != nil {
		ignoreFirstIntervals = *v
	} else {
		cfg.IgnoreFirstIntervals = &ignoreFirstIntervals
	}

	// initialize outputs list
	if len(cfg.Outputs) == 0 {
		return nil, fmt.Errorf("`outputs` list must contain at least a single entry from the list %s; "+
			"see https://docs.victoriametrics.com/stream-aggregation/", supportedOutputs)
	}
	slices.Sort(cfg.Outputs)
	cfg.Outputs = slices.Compact(cfg.Outputs)
	aggrStates := make([]aggrState, len(cfg.Outputs))
	for i, output := range cfg.Outputs {
		if strings.HasPrefix(output, "quantiles(") {
			if !strings.HasSuffix(output, ")") {
				return nil, fmt.Errorf("missing closing brace for `quantiles()` output")
			}
			argsStr := output[len("quantiles(") : len(output)-1]
			if len(argsStr) == 0 {
				return nil, fmt.Errorf("`quantiles()` must contain at least one phi")
			}
			args := strings.Split(argsStr, ",")
			phis := make([]float64, len(args))
			for j, arg := range args {
				arg = strings.TrimSpace(arg)
				phi, err := strconv.ParseFloat(arg, 64)
				if err != nil {
					return nil, fmt.Errorf("cannot parse phi=%q for quantiles(%s): %w", arg, argsStr, err)
				}
				if phi < 0 || phi > 1 {
					return nil, fmt.Errorf("phi inside quantiles(%s) must be in the range [0..1]; got %v", argsStr, phi)
				}
				phis[j] = phi
			}
			aggrStates[i] = newQuantilesAggrState(phis)
			continue
		}
		switch output {
		case "total":
			aggrStates[i] = newTotalAggrState(stalenessInterval, false, true)
		case "total_prometheus":
			aggrStates[i] = newTotalAggrState(stalenessInterval, false, false)
		case "increase":
			aggrStates[i] = newTotalAggrState(stalenessInterval, true, true)
		case "increase_prometheus":
			aggrStates[i] = newTotalAggrState(stalenessInterval, true, false)
		case "rate_sum":
			aggrStates[i] = newRateAggrState(stalenessInterval, "rate_sum")
		case "rate_avg":
			aggrStates[i] = newRateAggrState(stalenessInterval, "rate_avg")
		case "count_series":
			aggrStates[i] = newCountSeriesAggrState()
		case "count_samples":
			aggrStates[i] = newCountSamplesAggrState()
		case "unique_samples":
			aggrStates[i] = newUniqueSamplesAggrState()
		case "sum_samples":
			aggrStates[i] = newSumSamplesAggrState()
		case "last":
			aggrStates[i] = newLastAggrState()
		case "min":
			aggrStates[i] = newMinAggrState()
		case "max":
			aggrStates[i] = newMaxAggrState()
		case "avg":
			aggrStates[i] = newAvgAggrState()
		case "stddev":
			aggrStates[i] = newStddevAggrState()
		case "stdvar":
			aggrStates[i] = newStdvarAggrState()
		case "histogram_bucket":
			aggrStates[i] = newHistogramBucketAggrState(stalenessInterval)
		default:
			return nil, fmt.Errorf("unsupported output=%q; supported values: %s; "+
				"see https://docs.victoriametrics.com/stream-aggregation/", output, supportedOutputs)
		}
	}

	// initialize suffix to add to metric names after aggregation
	suffix := ":" + cfg.Interval
	if labels := removeUnderscoreName(by); len(labels) > 0 {
		suffix += fmt.Sprintf("_by_%s", strings.Join(labels, "_"))
	}
	if labels := removeUnderscoreName(without); len(labels) > 0 {
		suffix += fmt.Sprintf("_without_%s", strings.Join(labels, "_"))
	}
	suffix += "_"

	// initialize the aggregator
	a := &aggregator{
		match: cfg.Match,

		dropInputLabels:  dropInputLabels,
		inputRelabeling:  inputRelabeling,
		outputRelabeling: outputRelabeling,

		keepMetricNames:  keepMetricNames,
		ignoreOldSamples: ignoreOldSamples,

		by:                  by,
		without:             without,
		aggregateOnlyByTime: aggregateOnlyByTime,

		aggrStates: aggrStates,

		suffix: suffix,

		stopCh: make(chan struct{}),

		flushDuration:      ms.GetOrCreateHistogram(`vm_streamaggr_flush_duration_seconds`),
		dedupFlushDuration: ms.GetOrCreateHistogram(`vm_streamaggr_dedup_flush_duration_seconds`),

		flushTimeouts:      ms.GetOrCreateCounter(`vm_streamaggr_flush_timeouts_total`),
		dedupFlushTimeouts: ms.GetOrCreateCounter(`vm_streamaggr_dedup_flush_timeouts_total`),
	}
	if dedupInterval > 0 {
		a.da = newDedupAggr()
	}

	noAlignFlushToInterval := opts.NoAlignFlushToInterval
	if v := cfg.NoAlignFlushToInterval; v != nil {
		noAlignFlushToInterval = *v
	} else {
		cfg.NoAlignFlushToInterval = &noAlignFlushToInterval
	}

	flushOnShutdown := opts.FlushOnShutdown
	if v := cfg.FlushOnShutdown; v != nil {
		flushOnShutdown = *v
	} else {
		cfg.FlushOnShutdown = &flushOnShutdown
	}

	data, err := json.Marshal(&cfg)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal config: %w", err)
	}
	a.configHash = xxhash.Sum64(data)

	a.wg.Add(1)
	go func() {
		a.runFlusher(pushFunc, !noAlignFlushToInterval, !flushOnShutdown, interval, dedupInterval, ignoreFirstIntervals)
		a.wg.Done()
	}()

	return a, nil
}

func (a *aggregator) runFlusher(pushFunc PushFunc, alignFlushToInterval, skipIncompleteFlush bool, interval, dedupInterval time.Duration, ignoreFirstIntervals int) {
	alignedSleep := func(d time.Duration) {
		if !alignFlushToInterval {
			return
		}

		ct := time.Duration(time.Now().UnixNano())
		dSleep := d - (ct % d)
		timer := timerpool.Get(dSleep)
		defer timer.Stop()
		select {
		case <-a.stopCh:
		case <-timer.C:
		}
	}

	tickerWait := func(t *time.Ticker) bool {
		select {
		case <-a.stopCh:
			return false
		case <-t.C:
			return true
		}
	}

	if dedupInterval <= 0 {
		alignedSleep(interval)
		t := time.NewTicker(interval)
		defer t.Stop()

		if alignFlushToInterval && skipIncompleteFlush {
			a.flush(nil, interval, true)
		}

		for tickerWait(t) {
			pf := pushFunc
			if ignoreFirstIntervals > 0 {
				pf = nil
				ignoreFirstIntervals--
			}
			a.flush(pf, interval, true)

			if alignFlushToInterval {
				select {
				case <-t.C:
				default:
				}
			}
		}
	} else {
		alignedSleep(dedupInterval)
		t := time.NewTicker(dedupInterval)
		defer t.Stop()

		flushDeadline := time.Now().Add(interval)
		isSkippedFirstFlush := false
		for tickerWait(t) {
			a.dedupFlush(dedupInterval)

			ct := time.Now()
			if ct.After(flushDeadline) {
				pf := pushFunc
				if ignoreFirstIntervals > 0 {
					pf = nil
					ignoreFirstIntervals--
				}
				// It is time to flush the aggregated state
				if alignFlushToInterval && skipIncompleteFlush && !isSkippedFirstFlush {
					pf = nil
					isSkippedFirstFlush = true
				}
				a.flush(pf, interval, true)
				for ct.After(flushDeadline) {
					flushDeadline = flushDeadline.Add(interval)
				}
			}

			if alignFlushToInterval {
				select {
				case <-t.C:
				default:
				}
			}
		}
	}

	if !skipIncompleteFlush && ignoreFirstIntervals == 0 {
		a.dedupFlush(dedupInterval)
		a.flush(pushFunc, interval, true)
	}
}

func (a *aggregator) dedupFlush(dedupInterval time.Duration) {
	if dedupInterval <= 0 {
		// The de-duplication is disabled.
		return
	}

	startTime := time.Now()

	a.da.flush(a.pushSamples, true)

	d := time.Since(startTime)
	a.dedupFlushDuration.Update(d.Seconds())
	if d > dedupInterval {
		a.dedupFlushTimeouts.Inc()
		logger.Warnf("deduplication couldn't be finished in the configured dedup_interval=%s; it took %.03fs; "+
			"possible solutions: increase dedup_interval; use match filter matching smaller number of series; "+
			"reduce samples' ingestion rate to stream aggregation", dedupInterval, d.Seconds())
	}
}

func (a *aggregator) flush(pushFunc PushFunc, interval time.Duration, resetState bool) {
	startTime := time.Now()

	// Update minTimestamp before flushing samples to the storage,
	// since the flush durtion can be quite long.
	// This should prevent from dropping samples with old timestamps when the flush takes long time.
	a.minTimestamp.Store(startTime.UnixMilli() - 5_000)

	var wg sync.WaitGroup
	for _, as := range a.aggrStates {
		flushConcurrencyCh <- struct{}{}
		wg.Add(1)
		go func(as aggrState) {
			defer func() {
				<-flushConcurrencyCh
				wg.Done()
			}()

			ctx := getFlushCtx(a, pushFunc)
			as.flushState(ctx, resetState)
			ctx.flushSeries()
			ctx.resetSeries()
			putFlushCtx(ctx)
		}(as)
	}
	wg.Wait()

	d := time.Since(startTime)
	a.flushDuration.Update(d.Seconds())
	if d > interval {
		a.flushTimeouts.Inc()
		logger.Warnf("stream aggregation couldn't be finished in the configured interval=%s; it took %.03fs; "+
			"possible solutions: increase interval; use match filter matching smaller number of series; "+
			"reduce samples' ingestion rate to stream aggregation", interval, d.Seconds())
	}
}

var flushConcurrencyCh = make(chan struct{}, cgroup.AvailableCPUs())

// MustStop stops the aggregator.
//
// The aggregator stops pushing the aggregated metrics after this call.
func (a *aggregator) MustStop() {
	close(a.stopCh)
	a.wg.Wait()
}

// Push pushes tss to a.
func (a *aggregator) Push(tss []prompbmarshal.TimeSeries, matchIdxs []byte) {
	ctx := getPushCtx()
	defer putPushCtx(ctx)

	samples := ctx.samples
	buf := ctx.buf
	labels := &ctx.labels
	inputLabels := &ctx.inputLabels
	outputLabels := &ctx.outputLabels

	dropLabels := a.dropInputLabels
	ignoreOldSamples := a.ignoreOldSamples
	minTimestamp := a.minTimestamp.Load()
	for idx, ts := range tss {
		if !a.match.Match(ts.Labels) {
			continue
		}
		matchIdxs[idx] = 1

		if len(dropLabels) > 0 {
			labels.Labels = dropSeriesLabels(labels.Labels[:0], ts.Labels, dropLabels)
		} else {
			labels.Labels = append(labels.Labels[:0], ts.Labels...)
		}
		labels.Labels = a.inputRelabeling.Apply(labels.Labels, 0)
		if len(labels.Labels) == 0 {
			// The metric has been deleted by the relabeling
			continue
		}
		labels.Sort()

		inputLabels.Reset()
		outputLabels.Reset()
		if !a.aggregateOnlyByTime {
			inputLabels.Labels, outputLabels.Labels = getInputOutputLabels(inputLabels.Labels, outputLabels.Labels, labels.Labels, a.by, a.without)
		} else {
			outputLabels.Labels = append(outputLabels.Labels, labels.Labels...)
		}

		buf = compressLabels(buf[:0], inputLabels.Labels, outputLabels.Labels)
		key := bytesutil.InternBytes(buf)
		for _, sample := range ts.Samples {
			if math.IsNaN(sample.Value) {
				// Skip NaN values
				continue
			}
			if ignoreOldSamples && sample.Timestamp < minTimestamp {
				// Skip old samples outside the current aggregation interval
				continue
			}
			samples = append(samples, pushSample{
				key:       key,
				value:     sample.Value,
				timestamp: sample.Timestamp,
			})
		}
	}
	ctx.samples = samples
	ctx.buf = buf

	if a.da != nil {
		a.da.pushSamples(samples)
	} else {
		a.pushSamples(samples)
	}
}

func compressLabels(dst []byte, inputLabels, outputLabels []prompbmarshal.Label) []byte {
	bb := bbPool.Get()
	bb.B = lc.Compress(bb.B, inputLabels)
	dst = encoding.MarshalVarUint64(dst, uint64(len(bb.B)))
	dst = append(dst, bb.B...)
	bbPool.Put(bb)
	dst = lc.Compress(dst, outputLabels)
	return dst
}

func decompressLabels(dst []prompbmarshal.Label, key string) []prompbmarshal.Label {
	return lc.Decompress(dst, bytesutil.ToUnsafeBytes(key))
}

func getOutputKey(key string) string {
	src := bytesutil.ToUnsafeBytes(key)
	inputKeyLen, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		logger.Panicf("BUG: cannot unmarshal inputKeyLen from uvarint")
	}
	src = src[nSize:]
	outputKey := src[inputKeyLen:]
	return bytesutil.ToUnsafeString(outputKey)
}

func getInputOutputKey(key string) (string, string) {
	src := bytesutil.ToUnsafeBytes(key)
	inputKeyLen, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		logger.Panicf("BUG: cannot unmarshal inputKeyLen from uvarint")
	}
	src = src[nSize:]
	inputKey := src[:inputKeyLen]
	outputKey := src[inputKeyLen:]
	return bytesutil.ToUnsafeString(inputKey), bytesutil.ToUnsafeString(outputKey)
}

func (a *aggregator) pushSamples(samples []pushSample) {
	for _, as := range a.aggrStates {
		as.pushSamples(samples)
	}
}

type pushCtx struct {
	samples      []pushSample
	labels       promutils.Labels
	inputLabels  promutils.Labels
	outputLabels promutils.Labels
	buf          []byte
}

func (ctx *pushCtx) reset() {
	clear(ctx.samples)
	ctx.samples = ctx.samples[:0]

	ctx.labels.Reset()
	ctx.inputLabels.Reset()
	ctx.outputLabels.Reset()
	ctx.buf = ctx.buf[:0]
}

type pushSample struct {
	key       string
	value     float64
	timestamp int64
}

func getPushCtx() *pushCtx {
	v := pushCtxPool.Get()
	if v == nil {
		return &pushCtx{}
	}
	return v.(*pushCtx)
}

func putPushCtx(ctx *pushCtx) {
	ctx.reset()
	pushCtxPool.Put(ctx)
}

var pushCtxPool sync.Pool

func getInputOutputLabels(dstInput, dstOutput, labels []prompbmarshal.Label, by, without []string) ([]prompbmarshal.Label, []prompbmarshal.Label) {
	if len(without) > 0 {
		for _, label := range labels {
			if slices.Contains(without, label.Name) {
				dstInput = append(dstInput, label)
			} else {
				dstOutput = append(dstOutput, label)
			}
		}
	} else {
		for _, label := range labels {
			if !slices.Contains(by, label.Name) {
				dstInput = append(dstInput, label)
			} else {
				dstOutput = append(dstOutput, label)
			}
		}
	}
	return dstInput, dstOutput
}

func getFlushCtx(a *aggregator, pushFunc PushFunc) *flushCtx {
	v := flushCtxPool.Get()
	if v == nil {
		v = &flushCtx{}
	}
	ctx := v.(*flushCtx)
	ctx.a = a
	ctx.pushFunc = pushFunc
	return ctx
}

func putFlushCtx(ctx *flushCtx) {
	ctx.reset()
	flushCtxPool.Put(ctx)
}

var flushCtxPool sync.Pool

type flushCtx struct {
	a        *aggregator
	pushFunc PushFunc

	tss     []prompbmarshal.TimeSeries
	labels  []prompbmarshal.Label
	samples []prompbmarshal.Sample
}

func (ctx *flushCtx) reset() {
	ctx.a = nil
	ctx.pushFunc = nil
	ctx.resetSeries()
}

func (ctx *flushCtx) resetSeries() {
	clear(ctx.tss)
	ctx.tss = ctx.tss[:0]

	clear(ctx.labels)
	ctx.labels = ctx.labels[:0]

	ctx.samples = ctx.samples[:0]
}

func (ctx *flushCtx) flushSeries() {
	tss := ctx.tss
	if len(tss) == 0 {
		// nothing to flush
		return
	}

	outputRelabeling := ctx.a.outputRelabeling
	if outputRelabeling == nil {
		// Fast path - push the output metrics.
		if ctx.pushFunc != nil {
			ctx.pushFunc(tss)
		}
		return
	}

	// Slow path - apply output relabeling and then push the output metrics.
	auxLabels := promutils.GetLabels()
	dstLabels := auxLabels.Labels[:0]
	dst := tss[:0]
	for _, ts := range tss {
		dstLabelsLen := len(dstLabels)
		dstLabels = append(dstLabels, ts.Labels...)
		dstLabels = outputRelabeling.Apply(dstLabels, dstLabelsLen)
		if len(dstLabels) == dstLabelsLen {
			// The metric has been deleted by the relabeling
			continue
		}
		ts.Labels = dstLabels[dstLabelsLen:]
		dst = append(dst, ts)
	}
	if ctx.pushFunc != nil {
		ctx.pushFunc(dst)
	}
	auxLabels.Labels = dstLabels
	promutils.PutLabels(auxLabels)
}

func (ctx *flushCtx) appendSeries(key, suffix string, timestamp int64, value float64) {
	labelsLen := len(ctx.labels)
	samplesLen := len(ctx.samples)
	ctx.labels = decompressLabels(ctx.labels, key)
	if !ctx.a.keepMetricNames {
		ctx.labels = addMetricSuffix(ctx.labels, labelsLen, ctx.a.suffix, suffix)
	}
	ctx.samples = append(ctx.samples, prompbmarshal.Sample{
		Timestamp: timestamp,
		Value:     value,
	})
	ctx.tss = append(ctx.tss, prompbmarshal.TimeSeries{
		Labels:  ctx.labels[labelsLen:],
		Samples: ctx.samples[samplesLen:],
	})

	// Limit the maximum length of ctx.tss in order to limit memory usage.
	if len(ctx.tss) >= 10_000 {
		ctx.flushSeries()
		ctx.resetSeries()
	}
}

func (ctx *flushCtx) appendSeriesWithExtraLabel(key, suffix string, timestamp int64, value float64, extraName, extraValue string) {
	labelsLen := len(ctx.labels)
	samplesLen := len(ctx.samples)
	ctx.labels = decompressLabels(ctx.labels, key)
	if !ctx.a.keepMetricNames {
		ctx.labels = addMetricSuffix(ctx.labels, labelsLen, ctx.a.suffix, suffix)
	}
	ctx.labels = append(ctx.labels, prompbmarshal.Label{
		Name:  extraName,
		Value: extraValue,
	})
	ctx.samples = append(ctx.samples, prompbmarshal.Sample{
		Timestamp: timestamp,
		Value:     value,
	})
	ctx.tss = append(ctx.tss, prompbmarshal.TimeSeries{
		Labels:  ctx.labels[labelsLen:],
		Samples: ctx.samples[samplesLen:],
	})
}

func addMetricSuffix(labels []prompbmarshal.Label, offset int, firstSuffix, lastSuffix string) []prompbmarshal.Label {
	src := labels[offset:]
	for i := range src {
		label := &src[i]
		if label.Name != "__name__" {
			continue
		}
		bb := bbPool.Get()
		bb.B = append(bb.B, label.Value...)
		bb.B = append(bb.B, firstSuffix...)
		bb.B = append(bb.B, lastSuffix...)
		label.Value = bytesutil.InternBytes(bb.B)
		bbPool.Put(bb)
		return labels
	}
	// The __name__ isn't found. Add it
	bb := bbPool.Get()
	bb.B = append(bb.B, firstSuffix...)
	bb.B = append(bb.B, lastSuffix...)
	labelValue := bytesutil.InternBytes(bb.B)
	labels = append(labels, prompbmarshal.Label{
		Name:  "__name__",
		Value: labelValue,
	})
	return labels
}

func addMissingUnderscoreName(labels []string) []string {
	result := []string{"__name__"}
	for _, s := range labels {
		if s == "__name__" {
			continue
		}
		result = append(result, s)
	}
	return result
}

func removeUnderscoreName(labels []string) []string {
	var result []string
	for _, s := range labels {
		if s == "__name__" {
			continue
		}
		result = append(result, s)
	}
	return result
}

func sortAndRemoveDuplicates(a []string) []string {
	if len(a) == 0 {
		return nil
	}
	a = append([]string{}, a...)
	sort.Strings(a)
	dst := a[:1]
	for _, v := range a[1:] {
		if v != dst[len(dst)-1] {
			dst = append(dst, v)
		}
	}
	return dst
}

var bbPool bytesutil.ByteBufferPool
