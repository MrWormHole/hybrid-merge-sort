package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
)

//PART 1: WHY CONCURRENY IS NOT ALWAYS GREAT IN SOME CASES!
// Note: Pure sequential programs might be better than pure concurrent programs
// Subject: Merge Sort

// Merge merges left and right slices
func Merge(left []int, right []int) []int {
	result := make([]int, len(left)+len(right))

	i := 0
	for len(left) > 0 && len(right) > 0 {
		if left[0] < right[0] {
			result[i] = left[0]
			left = left[1:]
		} else {
			result[i] = right[0]
			right = right[1:]
		}
		i++
	}
	for j := 0; j < len(left); j++ {
		result[i] = left[j]
		i++
	}
	for j := 0; j < len(right); j++ {
		result[i] = right[j]
		i++
	}
	return result
}

//MergeSortParallel applies merge sort strategy via concurrency and channels
func MergeSortParallel(data []int, res chan []int) {
	if len(data) == 1 {
		res <- data
		return
	}

	leftChannel := make(chan []int)
	rightChannel := make(chan []int)
	middle := len(data) / 2

	go MergeSortParallel(data[:middle], leftChannel)
	go MergeSortParallel(data[middle:], rightChannel)

	left := <-leftChannel
	right := <-rightChannel

	close(leftChannel)
	close(rightChannel)
	res <- Merge(left, right)
}

//MergeSortSequential applies merge sort without any concurrency
func MergeSortSequential(items []int) []int {
	var num = len(items)

	if len(items) == 1 {
		return items
	}

	middle := int(num / 2)
	var (
		left  = make([]int, middle)
		right = make([]int, num-middle)
	)
	for i := 0; i < num; i++ {
		if i < middle {
			left[i] = items[i]
		} else {
			right[i-middle] = items[i]
		}
	}

	return Merge(MergeSortSequential(left), MergeSortSequential(right))
}

//PART 2: WHY CONCURRENY IS ALWAYS GREAT TO OPTIMIZE THINGS!
// Note: Pure sequential programs might be better than ever with integrated concurrent programs
// Subject: Merge Sort

func merge(s []int, middle int) {
	helper := make([]int, len(s))
	copy(helper, s)

	helperLeft := 0
	helperRight := middle
	current := 0
	high := len(s) - 1

	for helperLeft <= middle-1 && helperRight <= high {
		if helper[helperLeft] <= helper[helperRight] {
			s[current] = helper[helperLeft]
			helperLeft++
		} else {
			s[current] = helper[helperRight]
			helperRight++
		}
		current++
	}

	for helperLeft <= middle-1 {
		s[current] = helper[helperLeft]
		current++
		helperLeft++
	}
}

/* Optimized Sequential only */
func mergesort(s []int) {
	if len(s) > 1 {
		middle := len(s) / 2
		mergesort(s[:middle])
		mergesort(s[middle:])
		merge(s, middle)
	}
}

/* Optimized Parallel only */
func mergesortv1(s []int) {
	len := len(s)

	if len > 1 {
		middle := len / 2

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			mergesortv1(s[:middle])
		}()

		go func() {
			defer wg.Done()
			mergesortv1(s[middle:])
		}()

		wg.Wait()
		merge(s, middle)
	}
}

const threshold = 2048 * 32 //2048

/* Optimized Sequential and Parallel with 2 goroutines */
func mergesortv2(s []int) {
	len := len(s)

	if len > 1 {
		if len <= threshold { // Sequential
			mergesort(s)
		} else { // Parallel
			middle := len / 2

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				mergesortv2(s[:middle])
			}()

			go func() {
				defer wg.Done()
				mergesortv2(s[middle:])
			}()

			wg.Wait()
			merge(s, middle)
		}
	}
}

/* Optimized Sequential and Parallel with 1 goroutine */
func mergesortv3(s []int) {
	len := len(s)

	if len > 1 {
		if len <= threshold { // Sequential
			mergesort(s)
		} else { // Parallel
			middle := len / 2

			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				defer wg.Done()
				mergesortv3(s[:middle])
			}()

			mergesortv3(s[middle:])

			wg.Wait()
			merge(s, middle)
		}
	}
}

func main() {
	fmt.Println("Threshold for hybrid algorithm is: ", threshold)
	fmt.Println("Number of elements for sorting: 10M")
	fmt.Println("Number of available logical CPU cores in this system: ", runtime.NumCPU())
	size := 10000000
	sample := make([]int, size)
	sample2 := make([]int, size)
	sample3 := make([]int, size)
	sample4 := make([]int, size)
	sample5 := make([]int, size)
	sample6 := make([]int, size)
	source := rand.NewSource(time.Now().UnixNano())
	randomizer := rand.New(source)
	for i := 0; i < size; i++ {
		sample[i] = randomizer.Intn(1000)
	}
	copy(sample2, sample)
	copy(sample3, sample)
	copy(sample4, sample)
	copy(sample5, sample)
	copy(sample6, sample)

	// Pure parallel way starts(does NOT modify given sample)
	fmt.Println(sort.SliceIsSorted(sample, func(i int, j int) bool { return sample[i] < sample[j] }))
	resultChannel := make(chan []int)
	start := time.Now()
	go MergeSortParallel(sample, resultChannel)
	resultArray := <-resultChannel
	elapsed := time.Since(start)
	fmt.Printf("Merge Sort(parallel only) took %s\n", elapsed)
	close(resultChannel)
	fmt.Println(sort.SliceIsSorted(resultArray, func(i int, j int) bool { return resultArray[i] < resultArray[j] }))
	// Pure parallel way ends

	// Pure sequential way starts(does NOT modify given sample)
	fmt.Println(sort.SliceIsSorted(sample2, func(i int, j int) bool { return sample2[i] < sample2[j] }))
	start2 := time.Now()
	resultArray2 := MergeSortSequential(sample2)
	elapsed2 := time.Since(start2)
	fmt.Printf("Merge Sort(sequential only) took %s\n", elapsed2)
	fmt.Println(sort.SliceIsSorted(resultArray2, func(i int, j int) bool { return resultArray2[i] < resultArray2[j] }))
	// Pure sequential ways ends

	// Optimized pure sequential way starts(does modify given sample)
	fmt.Println(sort.SliceIsSorted(sample3, func(i int, j int) bool { return sample3[i] < sample3[j] }))
	start3 := time.Now()
	mergesort(sample3)
	elapsed3 := time.Since(start3)
	fmt.Printf("Merge Sort(optimized sequential only) took %s\n", elapsed3)
	fmt.Println(sort.SliceIsSorted(sample3, func(i int, j int) bool { return sample3[i] < sample3[j] }))
	// Optimized pure sequential way ends

	// Optimized pure parallel way starts(does modify given sample)
	fmt.Println(sort.SliceIsSorted(sample4, func(i int, j int) bool { return sample4[i] < sample4[j] }))
	start4 := time.Now()
	mergesortv1(sample4)
	elapsed4 := time.Since(start4)
	fmt.Printf("Merge Sort(optimized parallel only) took %s\n", elapsed4)
	fmt.Println(sort.SliceIsSorted(sample4, func(i int, j int) bool { return sample4[i] < sample4[j] }))
	// Optimized pure parallel way ends

	// Optimized hybrid way starts(does modify given sample)
	fmt.Println(sort.SliceIsSorted(sample5, func(i int, j int) bool { return sample5[i] < sample5[j] }))
	start5 := time.Now()
	mergesortv2(sample5)
	elapsed5 := time.Since(start5)
	fmt.Printf("Merge Sort(optimized hybrid with 2 goroutines only) took %s\n", elapsed5)
	fmt.Println(sort.SliceIsSorted(sample5, func(i int, j int) bool { return sample5[i] < sample5[j] }))
	// Optimized hybrid way ends

	// Optimized hybrid way starts(does modify given sample)
	fmt.Println(sort.SliceIsSorted(sample6, func(i int, j int) bool { return sample6[i] < sample6[j] }))
	start6 := time.Now()
	mergesortv2(sample6)
	elapsed6 := time.Since(start6)
	fmt.Printf("Merge Sort(optimized hybrid with 1 goroutine only) took %s\n", elapsed6)
	fmt.Println(sort.SliceIsSorted(sample6, func(i int, j int) bool { return sample6[i] < sample6[j] }))
	// Optimized hybrid way ends
}
