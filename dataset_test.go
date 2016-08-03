package hdrbench

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func fuzzyEquals(t *testing.T, expected, actual float64) bool {
	if !assert.InEpsilon(t, expected, actual, 0.000001) {
		t.Error(expected, actual, "Not withing epsilon")
		return false
	}
	return true
}

var s1 = []float64{0.123, 0.01, 0.43, 0.41, 0.415, 0.2201, 0.3201, 0.125, 0.13}

func TestDatasetMean(t *testing.T) {
	e := NewDataset("meantest", s1, 0.0, 0.0)
	mean := e.Mean()
	fuzzyEquals(t, 0.24257777, mean)
	meanInt := e.MeanInt(100.0)
	assert.Equal(t, int64(24), meanInt)
}

func TestDatasetMax(t *testing.T) {
	e := NewDataset("maxtest", s1, 0.0, 0.0)
	max := e.Max()
	fuzzyEquals(t, 0.43, max)
	maxInt := e.MaxInt(100.0)
	assert.Equal(t, int64(43), maxInt)
}

func TestDatasetMin(t *testing.T) {
	e := NewDataset("mintest", s1, 0.0, 0.0)
	min := e.Min()
	fuzzyEquals(t, 0.01, min)
	minInt := e.MinInt(100.0)
	assert.Equal(t, int64(1), minInt)
}

func helpDatasetQTest(t *testing.T, vals, qin, qexpect []float64) {
	e := NewDataset("test", vals, 0.0, 0.0)
	e.Sort()
	qout := Quantiles(e.dataset, qin)
	if len(qout) != len(qexpect) {
		t.Errorf("wrong number of quantiles")
	}
	for i, q := range qout {
		fuzzyEquals(t, qexpect[i], q)
	}
}
func TestDatasetQuantiles(t *testing.T) {
	helpDatasetQTest(t, []float64{1}, []float64{0, 0.25, 0.5, 1}, []float64{1, 1, 1, 1})
	helpDatasetQTest(t, s1,
		[]float64{0, 0.25, 0.50, 0.95, 0.99, 1.0},
		[]float64{0.01, 0.125, 0.3201, 0.43, 0.43, 0.43})
	helpDatasetQTest(t, []float64{1.0, 2.0}, []float64{0.5}, []float64{2.0})
}
