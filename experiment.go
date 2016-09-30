package hdrbench

import (
	"fmt"
	"math"
	"sync"

	"github.com/go-errors/errors"
	"github.com/octo47/hdrbench/circonusllhist"

	"github.com/codahale/hdrhistogram"
)

type Histogram interface {
	Name() string
	// Calculate quantiles
	Quantiles(qin []float64) ([]float64, error)
	ValueAtQuantile(qin float64) int64
	SignificantFigures() int64
	UsedMem() int64
	Reset()
	RecordValues(datasets []*Dataset, start, stop int) error
}

type circonusHistogram struct {
	merged *circonusllhist.Histogram
}

func NewCircosusHist() (Histogram, error) {
	return &circonusHistogram{
		merged: circonusllhist.New(),
	}, nil
}

func (hhist *circonusHistogram) Reset() {
	hhist.merged = circonusllhist.New()
}

func (hhist *circonusHistogram) RecordValues(
	datasets []*Dataset,
	start, stop int) error {

	results := make([]*circonusllhist.Histogram, len(datasets))
	errors := make([]error, len(datasets))
	wg := sync.WaitGroup{}
	for i, dataset := range datasets {
		wg.Add(1)
		go func(idx int, dataset *Dataset) {
			defer wg.Done()
			hist := circonusllhist.New()
			for _, v := range dataset.dataset[start:stop] {
				err := hist.RecordValue(v)
				if err != nil {
					errors[idx] = err
					return
				}
			}
			results[idx] = hist
		}(i, dataset)
	}
	wg.Wait()
	for i := range results {
		if errors[i] != nil {
			return errors[i]
		}
		hhist.merged.Merge(results[i])
	}
	return nil
}

func (hhist *circonusHistogram) ValueAtQuantile(qin float64) int64 {
	v := hhist.merged.ValueAtQuantile(qin)
	return int64(v)
}

func (hhist *circonusHistogram) Name() string {
	return "Circonus"
}

func (hhist *circonusHistogram) Quantiles(qin []float64) ([]float64, error) {
	return hhist.merged.ApproxQuantile(qin)
}

func (hhist *circonusHistogram) SignificantFigures() int64 {
	return hhist.merged.SignificantFigures()
}

func (hhist *circonusHistogram) UsedMem() int64 {
	return int64(hhist.merged.UsedMem())
}

type hdrHistogram struct {
	merged     *hdrhistogram.Histogram
	scaleToInt float64
}

func NewHdrHist(min int64, max int64, sigDigits int, scaleToInt float64) (Histogram, error) {

	return &hdrHistogram{
		merged:     hdrhistogram.New(min, max, sigDigits),
		scaleToInt: scaleToInt,
	}, nil
}

func (hhist *hdrHistogram) Name() string {
	return "HDR"
}

func (hhist *hdrHistogram) Reset() {
	hhist.merged = hdrhistogram.New(
		0, 10^6, int(hhist.merged.SignificantFigures()))
}

func (hhist *hdrHistogram) RecordValues(
	datasets []*Dataset,
	start, stop int) error {

	max := int64(math.MinInt64)
	for _, dataset := range datasets {
		if max < dataset.MaxInt(hhist.scaleToInt) {
			max = dataset.MaxInt(hhist.scaleToInt)
		}
	}

	if hhist.merged.HighestTrackableValue() < max {
		newMerged := hdrhistogram.New(
			0, max, int(hhist.merged.SignificantFigures()))
		dropped := newMerged.Merge(hhist.merged)
		if dropped != 0 {
			return errors.New(fmt.Sprintf("Dropped %d values during merge", dropped))
		}
		hhist.merged = newMerged
	}
	results := make([]*hdrhistogram.Histogram, len(datasets))
	errors := make([]error, len(datasets))
	wg := sync.WaitGroup{}
	for i, dataset := range datasets {
		wg.Add(1)
		go func(idx int, dataset *Dataset) {
			defer wg.Done()
			hist := hdrhistogram.New(
				// trying to keep same resolution as our main histogram
				hhist.merged.LowestTrackableValue(),
				hhist.merged.HighestTrackableValue(),
				int(hhist.merged.SignificantFigures()))
			for idx := start; idx < stop; idx++ {
				err := hist.RecordValue(dataset.IntValue(idx, hhist.scaleToInt))
				if err != nil {
					errors[idx] = err
					return
				}
			}
			results[idx] = hist
		}(i, dataset)
	}
	wg.Wait()
	for i := range results {
		if errors[i] != nil {
			return errors[i]
		}
		hhist.merged.Merge(results[i])
	}
	return nil
}

func (hhist *hdrHistogram) Quantiles(qin []float64) ([]float64, error) {
	qout := make([]float64, len(qin))
	for i := range qin {
		qout[i] = float64(hhist.merged.ValueAtQuantile(qin[i]*100.0)) / hhist.scaleToInt
	}
	return qout, nil
}

func (hhist *hdrHistogram) ValueAtQuantile(qin float64) int64 {
	v := hhist.merged.ValueAtQuantile(qin)
	return v
}

func (hhist *hdrHistogram) SignificantFigures() int64 {
	return hhist.merged.SignificantFigures()
}

func (hhist *hdrHistogram) UsedMem() int64 {
	return int64(hhist.merged.ByteSize())
}

type preciseHistogram struct {
	merged []float64
	sorted bool
}

func NewPreceiseHist() (Histogram, error) {
	return &preciseHistogram{
		merged: make([]float64, 0),
	}, nil
}

func (hhist *preciseHistogram) Reset() {
	hhist.merged = make([]float64, 0)
}

func (hhist *preciseHistogram) Name() string {
	return "Precise"
}

func (hhist *preciseHistogram) RecordValues(
	datasets []*Dataset,
	start, stop int) error {

	hhist.sorted = false
	for _, dataset := range datasets {
		hhist.merged = append(hhist.merged, dataset.dataset[start:stop]...)
	}
	return nil
}

func (hhist *preciseHistogram) Quantiles(qin []float64) ([]float64, error) {
	if !hhist.sorted {
		hhist.merged = QSortFloat(hhist.merged)
		hhist.sorted = true
	}
	return Quantiles(hhist.merged, qin), nil
}

func (hhist *preciseHistogram) ValueAtQuantile(qin float64) int64 {
	if !hhist.sorted {
		hhist.merged = QSortFloat(hhist.merged)
		hhist.sorted = true
	}
	_, count := Quantile(hhist.merged, qin)
	return count
}

func (hhist *preciseHistogram) SignificantFigures() int64 {
	return 2
}

func (hhist *preciseHistogram) UsedMem() int64 {
	return int64(8 * len(hhist.merged))
}
