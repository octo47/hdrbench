package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"text/tabwriter"

	"github.com/golang/glog"
	"github.com/octo47/hdrbench"
	"github.com/octo47/hdrbench/gnuplot"
)

var datapointsCount = flag.Int("datapoints", 240, "Num of datapoints per signal per iteration")
var iterationsCount = flag.Int("iter", 5, "Num of iterations")
var maxSignals = flag.Int("max-sig", 3000, "Num of parallel signals to simulate")
var minSignals = flag.Int("min-sig", 3, "Num of parallel signals to simulate")
var signalMultiplier = flag.Int("mult-proc", 30,
	"Increasing number of signals by multiplying by this")
var drawDatasets = flag.Bool("draw-dataset", false,
	"Draw datasets graphs (requires gnuplot)")
var drawErrors = flag.Bool("draw-errors", false,
	"Draw datasets graphs (requires gnuplot)")
var randSeed = flag.Int64("rand", 1234, "Random seed to use")
var outputDir = flag.String("workdir", ".", "Directory to put generated files to")
var intScale = flag.Float64("int-scale", 10.0, "How scale floats to int for some histograms")
var outliers = flag.Int("outliers", 1, "Number of high latency signal")

type HistogramList []hdrbench.Histogram

type quantileDiffHistory [][][]float64

func main() {
	flag.Parse()

	mustBeDir(*outputDir)

	var err error
	var hist hdrbench.Histogram
	histograms := make(HistogramList, 0)
	if hist, err = hdrbench.NewPreceiseHist(); err != nil {
		glog.Fatal("Unable to create precise histo")
	}
	histograms = append(histograms, hist)
	glog.Info("Adding ", hist.Name(), " histogram")
	if hist, err = hdrbench.NewHdrHist(0, 1000, 2, *intScale); err != nil {
		glog.Fatal("Unable to create HDR histo")
	}
	histograms = append(histograms, hist)
	glog.Info("Adding ", hist.Name(), " histogram")
	if hist, err = hdrbench.NewCircosusHist(); err != nil {
		glog.Fatal("Unable to create Circonus histor")
	}
	histograms = append(histograms, hist)
	glog.Info("Adding ", hist.Name(), " histogram")

	for singals := *minSignals; singals <= (*maxSignals); singals *= *signalMultiplier {
		quantilesDiff := make(quantileDiffHistory, *iterationsCount)
		glog.Info("Caclulating errors for ", singals, " signals over ",
			*iterationsCount, " iterations")
		glog.Info("  each signal will recieve ", *datapointsCount, " datapoint per iteration")
		rnd := rand.New(rand.NewSource(*randSeed))
		datasets := hdrbench.NewLatencyDatasets(
			rnd, (*datapointsCount)*(*iterationsCount), singals, *outliers)
		if *drawDatasets {
			_ = hdrbench.PlotDatasets(datasets,
				path.Join(*outputDir, "signals"+strconv.Itoa(singals)+".png"),
				*datapointsCount)
		}
		for iter := 0; iter < *iterationsCount; iter++ {
			iterationQuantiles := make([][]float64, len(histograms))
			quantilesDiff[iter] = make([][]float64, len(histograms))
			for hi, hist := range histograms {
				hist.Reset()
				err := hist.RecordValues(
					datasets, iter*(*datapointsCount), (iter+1)*(*datapointsCount))
				if err != nil {
					glog.Fatalf("Failed to record values iter %d hist %s",
						iter, histograms[iter].Name())
				}
				iterationQuantiles[hi], err = hist.Quantiles(AllQuantiles)
				if err != nil {
					glog.Fatalf("Failed to calculate quantiles at iter %d hist %s",
						iter, histograms[iter].Name())
				}
			}
			for hi := 1; hi < len(histograms); hi++ {
				quantilesDiff[iter][hi] = hdrbench.DiffRelative(
					iterationQuantiles[0],
					iterationQuantiles[hi])
				quantilesDiff[iter][hi] = hdrbench.QSortFloat(quantilesDiff[iter][hi])
			}
		}
		glog.Info("Calculated ", singals, " signals")
		for _, histogram := range histograms {
			glog.Info("Histogram ", histogram.Name(), " uses ", histogram.UsedMem(), " bytes")
		}
		reportQuantilesErrors(singals, histograms, quantilesDiff)
	}
}

func reportQuantilesErrors(signals int, histograms HistogramList, quantilesDiff quantileDiffHistory) {
	glog.Info("Generating report")

	w := new(tabwriter.Writer)
	// Format in tab-separated columns with a tab stop of 8.
	w.Init(os.Stdout, 16, 8, 0, '\t', 0)
	// start from 1 due of histograms[0] is alwasy Precise
	for hIdx := 1; hIdx < len(histograms); hIdx++ {
		histogram := histograms[hIdx]
		fmt.Fprintln(w, histogram.Name())
		// print quantiles header
		for i := range errorQuantiles {
			fmt.Fprintf(w, "\t%0.2f", errorQuantiles[i])
		}
		fmt.Fprintln(w)
		graph := make(map[float64][]float64)
		for _, qval := range errorQuantiles {
			// making per quantile with size of number of iterations
			graph[qval] = make([]float64, len(quantilesDiff))
		}
		for iter := range quantilesDiff {
			fmt.Fprintf(w, "%d", iter+1)
			errorQ := hdrbench.Quantiles(quantilesDiff[iter][hIdx], errorQuantiles)
			for i := range errorQuantiles {
				fmt.Fprintf(w, "\t%.2f%%", errorQ[i]*100)
				graph[errorQuantiles[i]][iter] = errorQ[i] * 100
			}
			fmt.Fprintln(w)
		}
		if *drawErrors {
			// don't draw more then 300 graphs, gnuplot wouldn't be happy
			_ = plotErrors(signals, histogram, graph)
		}
	}
	w.Flush()
}

func plotErrors(signals int, hist hdrbench.Histogram, graph map[float64][]float64) error {
	p, err := gnuplot.NewPlotter("", false, false)
	if err != nil {
		return err
	}
	defer p.Close()

	fname := path.Join(*outputDir, hist.Name()+strconv.Itoa(signals)+".png")

	_ = p.SetStyle("lines")
	p.CheckedCmd("set terminal png")
	p.CheckedCmd("set title 'Approximated quantiles errors'")
	for k, v := range graph {
		err = p.PlotX(v, fmt.Sprintf("P%02.0f", k*100))
		if err != nil {
			return err
		}
	}
	p.CheckedCmd("set output '" + fname + "'")
	p.CheckedCmd("replot")

	p.CheckedCmd("q")
	return nil
}

var errorQuantiles = []float64{0.1, 0.5, 0.97, 0.99}

const (
	// 100/QuantilesCount will be precision. With 1000 it would be quantiles with 0.1 step
	QuantilesCount = 1000
)

var AllQuantiles = make([]float64, QuantilesCount)

func init() {
	for i := 1; i <= len(AllQuantiles); i++ {
		AllQuantiles[i-1] = float64(i) / float64(QuantilesCount) // every .1 pct
	}
}

func mustBeDir(dirname string) {
	if stat, err := os.Stat(dirname); err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirname, 0777)
			if err != nil {
				glog.Fatal("Unable to create directory ", dirname, err)
			}
		}
		if !stat.IsDir() {
			glog.Fatalf("%s exists, but not a directory", *outputDir)
		}
	}
	glog.Info("Using workdir ", dirname)
}
