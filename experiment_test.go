package hdrbench

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCirconusHist(t *testing.T) {
	hist, _ := NewCircosusHist()
	histTestHelper(t, hist)
}

func TestHdrHist(t *testing.T) {
	hist, _ := NewHdrHist(0, 10^6, 2, 100.0)
	histTestHelper(t, hist)
}

func histTestHelper(t *testing.T, hist Histogram) {
	rnd := rand.New(rand.NewSource(1234))
	dset1 := NewLatencyDataset("ds1", rnd.Int63(), rnd.Int63(), 0, 1200.0, 1000)
	dset2 := NewLatencyDataset("ds2", rnd.Int63(), rnd.Int63(), 0, 1200.0, 1000)
	phist, _ := NewPreceiseHist()
	hist.RecordValues([]*Dataset{dset1, dset2}, 0, 500)
	phist.RecordValues([]*Dataset{dset1, dset2}, 0, 500)
	hist.RecordValues([]*Dataset{dset1, dset2}, 500, 1000)
	phist.RecordValues([]*Dataset{dset1, dset2}, 500, 1000)
	quantiles := []float64{0.1, 0.5, 0.7, 0.95, 0.99}
	histQ, _ := hist.Quantiles(quantiles)
	phistQ, _ := phist.Quantiles(quantiles)
	histDiff := DiffRelative(phistQ, histQ)
	fmt.Println(phist.Name(), phistQ)
	fmt.Println(hist.Name(), histQ)
	fmt.Println(phist.Name(), "-", hist.Name(), histDiff)
	for _, diff := range histDiff {
		require.InDelta(t, 0.0, math.Abs(diff), 0.05)
	}
}

func BenchmarkCirconus(b *testing.B) {
	b.StopTimer()
	rnd := rand.New(rand.NewSource(1234))
	dset := NewLatencyDataset("ds", rnd.Int63(), rnd.Int63(), 0, 1200.0, b.N)
	hist, _ := NewCircosusHist()
	b.StartTimer()
	hist.RecordValues([]*Dataset{dset}, 0, b.N)
}

func BenchmarkHdr(b *testing.B) {
	b.StopTimer()
	rnd := rand.New(rand.NewSource(1234))
	dset := NewLatencyDataset("ds", rnd.Int63(), rnd.Int63(), 0, 1200.0, b.N)
	hist, _ := NewHdrHist(0, 10^6, 3, 1.0)
	b.StartTimer()
	hist.RecordValues([]*Dataset{dset}, 0, b.N)
}
