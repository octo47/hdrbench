package hdrbench

import (
	"math"
	"math/rand"
	"strconv"

	"github.com/octo47/hdrbench/gnuplot"
	"github.com/octo47/tsgen/generator"
)

type Dataset struct {
	name                   string
	dataset                []float64
	min, max               float64
	mean                   float64
	upperBound, lowerBound float64
}

func NewDataset(name string, dataset []float64, upper, lower float64) *Dataset {
	sum := 0.0
	min := math.Inf(1)
	max := math.Inf(-1)
	for _, v := range dataset {
		sum += v
		if min > v {
			min = v
		}
		if max < v {
			max = v
		}
	}
	if lower > min {
		lower = min
	}
	if upper < max {
		upper = max
	}
	mean := sum / float64(len(dataset))
	dset := make([]float64, len(dataset))
	copy(dset, dataset)
	return &Dataset{
		name:       name,
		min:        min,
		max:        max,
		mean:       mean,
		dataset:    dset,
		upperBound: upper,
		lowerBound: lower,
	}
}

func NewLatencyDataset(name string, baseSeed, mixinSeed int64, min, max float64, n int) *Dataset {
	baseRnd := rand.New(rand.NewSource(baseSeed))
	mixinRnd := rand.New(rand.NewSource(mixinSeed))
	points := make([]generator.Point, n)
	diff := max - min
	gen := generator.NewCombineGenerator(
		generator.NewRandomWalkGenerator(baseRnd, diff/10.0, min, max),
		[]generator.Generator{
			generator.NewRandomWalkGenerator(mixinRnd, diff/1000, -diff/10, diff/10),
		},
	)
	gen.Next(&points)
	values := make([]float64, n)
	for i, p := range points {
		values[i] = p.Value
	}
	return NewDataset(name, values, min, max)
}

func NewLatencyDatasets(rnd *rand.Rand, n int, datasets int, outliers int) []*Dataset {

	ds := make([]*Dataset, datasets+outliers)
	mixinSeed := rnd.Int63()
	for idx := 0; idx < datasets; idx++ {
		baseSeed := rnd.Int63()
		ds[idx] = NewLatencyDataset("lowLatency"+strconv.Itoa(idx), baseSeed, mixinSeed,
			1.0, 1500.0, n)
	}
	for idx := datasets; idx < datasets+outliers; idx++ {
		baseSeed := rnd.Int63()
		mixinSeed := rnd.Int63()
		minLat := float64(rnd.Intn(100))
		ds[idx] = NewLatencyDataset("highLatancy"+strconv.Itoa(idx), baseSeed, mixinSeed,
			minLat+1500.0, 10000.0, n)
	}
	return ds
}

func (fd *Dataset) UsedMem() int64 {
	return int64(len(fd.dataset) * 8)
}

func (fd *Dataset) IntValue(idx int, scaleToInt float64) int64 {
	return int64(Round(float64(fd.dataset[idx]) * scaleToInt))
}

func (e *Dataset) Mean() float64 {
	return e.mean
}

func (e *Dataset) Min() float64 {
	return e.min
}

func (e *Dataset) Max() float64 {
	return e.max
}

func (e *Dataset) UpperBound() float64 {
	return e.upperBound
}

func (e *Dataset) LowerBound() float64 {
	return e.lowerBound
}

func (e *Dataset) MeanInt(scaleToInt float64) int64 {
	return int64(Round(float64(e.mean) * scaleToInt))
}

func (e *Dataset) MinInt(scaleToInt float64) int64 {
	return int64(Round(float64(e.min) * scaleToInt))
}

func (e *Dataset) MaxInt(scaleToInt float64) int64 {
	return int64(Round(float64(e.max) * scaleToInt))
}

func (e *Dataset) Sort() {
	e.dataset = QSortFloat(e.dataset)
}

func PlotDatasets(ds []*Dataset, fname string, batchSize int) error {

	p, err := gnuplot.NewPlotter("", false, false)
	if err != nil {
		return err
	}
	defer p.Close()

	p.SetStyle("lines")
	p.CheckedCmd("set terminal png enhanced size 1280,1024")

	for dsI := range ds {
		data := Downsample(ds[dsI].dataset, batchSize, []float64{0.5, 0.0, 1.0})
		p.PlotXErrorBars(data, ds[dsI].name+strconv.Itoa(dsI))
	}
	p.CheckedCmd("set output '" + fname + "'")
	p.CheckedCmd("replot")

	p.CheckedCmd("q")
	return nil
}
