package hdrbench

import (
	"github.com/golang/glog"
	"math"
	"math/rand"
	"sort"
)

func Sum(numbers []float64) (total float64) {
	for _, x := range numbers {
		total += x
	}
	return total
}

func Mean(numbers []float64) float64 {
	return Sum(numbers) / float64(len(numbers))
}

func Median(numbers []float64) float64 {
	middle := len(numbers) / 2
	result := numbers[middle]
	if len(numbers)%2 == 0 {
		result = (result + numbers[middle-1]) / 2
	}
	return result
}

func Mode(numbers []float64) (modes []float64) {
	frequencies := make(map[float64]int, len(numbers))
	highestFrequency := 0
	for _, x := range numbers {
		frequencies[x]++
		if frequencies[x] > highestFrequency {
			highestFrequency = frequencies[x]
		}
	}
	for x, frequency := range frequencies {
		if frequency == highestFrequency {
			modes = append(modes, x)
		}
	}
	if highestFrequency == 1 || len(modes) == len(numbers) {
		modes = modes[:0] // Or: modes = []float64{}
	}
	sort.Float64s(modes)
	return modes
}

func StdDev(numbers []float64, mean float64) float64 {
	total := 0.0
	for _, number := range numbers {
		total += math.Pow(number-mean, 2)
	}
	variance := total / float64(len(numbers)-1)
	return math.Sqrt(variance)
}

func Quantile(numbers []float64, n float64) (float64, int64) {
	l := int64(len(numbers))
	position := Round(float64(l) * n)
	if position >= l {
		position = l - 1
	}
	return numbers[int(position)], int64(position)
}

func Quantiles(numbers []float64, n []float64) []float64 {
	result := make([]float64, len(n))
	for i, v := range n {
		result[i], _ = Quantile(numbers, v)
	}
	return result
}

func Diff(numbers []float64, numbersOther []float64) []float64 {
	rv := make([]float64, len(numbers))
	for i := range numbers {
		rv[i] = math.Abs(numbersOther[i] - numbers[i])
	}
	return rv
}

func DiffRelative(original []float64, derived []float64) []float64 {
	if len(original) != len(derived) {
		glog.Fatal("Expected same size of arrays")
	}
	epsilon := 10e-16
	rv := make([]float64, len(original))
	for i := range original {
		if original[i] < epsilon {
			rv[i] = 0.0
		} else {
			rv[i] = math.Abs(derived[i]-original[i]) / math.Abs(original[i])
		}
	}
	return rv
}

// Go doesn't have Round function :(
// https://github.com/golang/go/issues/4594
func Round(n float64) int64 {
	if n < 0 {
		return int64(math.Ceil(n - 0.5))
	}
	return int64(math.Floor(n + 0.5))
}

// Go uses interfaces for sorting, that is quite slow.
func QSortFloat(a []float64) []float64 {
	if len(a) < 2 {
		return a
	}

	left, right := 0, len(a)-1

	// Pick a pivot
	pivotIndex := rand.Int() % len(a)

	// Move the pivot to the right
	a[pivotIndex], a[right] = a[right], a[pivotIndex]

	// Pile elements smaller than the pivot on the left
	for i := range a {
		if a[i] < a[right] {
			a[i], a[left] = a[left], a[i]
			left++
		}
	}

	// Place the pivot after the last smaller element
	a[left], a[right] = a[right], a[left]

	// Go down the rabbit hole
	QSortFloat(a[:left])
	QSortFloat(a[left+1:])

	return a
}

// process in ranges, returning quantiles for every range.
func Downsample(values []float64, step int, q []float64) []float64 {
	qvals := make([]float64, 0)
	slice := make([]float64, 0, step)
	for i := 0; i < len(values); i += step {
		subLen := step
		if i+subLen > len(values) {
			subLen = len(values) - i
		}
		subSet := values[i : i+subLen]
		slice = slice[0:subLen]
		copy(slice, subSet)
		slice = QSortFloat(slice)
		for _, qv := range q {
			switch qv {
			case 1.0:
				qvals = append(qvals, slice[len(slice)-1])
			case 0.0:
				qvals = append(qvals, slice[0])
			default:
				qvals = append(qvals, Quantile(slice, qv))
			}
		}
	}
	return qvals
}
