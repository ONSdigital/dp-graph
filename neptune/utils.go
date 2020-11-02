package neptune

import (
	"strings"
	"sync"
)

// batchSize is the size of each batch for queries that are run concurrently in batches
const (
	batchSizeReader = 25000
	batchSizeWriter = 150
)

// maxWorkers is the maximum number of parallel go-routines that will trigger gremlin queries for a particular task
const maxWorkers = 150

// batchProcessor defines a generic function type to process a batch (array of strings) and may return a result (array of strings) and an error.
type batchProcessor = func([]string) ([]string, error)

// processInConcurrentBatches splits the provided items in batches and calls processBatch for each batch batch, concurrently.
// The results of the batch Processor functions, if provided, are aggregated as unique items and returned.
func processInConcurrentBatches(items []string, processBatch batchProcessor, batchSize int) (result map[string]struct{}, numChunks int, errs []error) {
	wg := sync.WaitGroup{}
	chWait := make(chan struct{})
	chErr := make(chan error)
	chSemaphore := make(chan struct{}, maxWorkers)

	result = make(map[string]struct{})
	lockResult := sync.Mutex{}

	// worker add delta to workgroup and acquire semaphore
	acquire := func() {
		wg.Add(1)
		chSemaphore <- struct{}{}
	}

	// worker release semaphore and workgroup delta
	release := func() {
		<-chSemaphore
		wg.Done()
	}

	// func executed in each go-routine to process the batch, aggregate results, and send errors to the error channel
	doProcessBatch := func(chunk []string) {
		defer release()
		res, err := processBatch(chunk)
		if err != nil {
			chErr <- err
			return
		}
		lockResult.Lock()
		for _, resItem := range res {
			result[resItem] = struct{}{}
		}
		lockResult.Unlock()
	}

	// func that triggers the batch processing for a chunk, in a parallel go-routine
	goProcessBatch := func(chunk []string) {
		acquire()
		go doProcessBatch(chunk)
	}

	// split in batches, and trigger a go-routine for each batch
	numChunks = processInBatches(items, goProcessBatch, batchSize)

	// func that will close wait channel when all go-routines complete their execution
	go func() {
		wg.Wait()
		close(chWait)
	}()

	// Block until all workers finish their work, keeping track of errors
	for {
		select {
		case err := <-chErr:
			errs = append(errs, err)
		case <-chWait:
			return
		}
	}
}

// processInBatches is an aux function that splits the provided items in batches and calls processBatch for each batch
func processInBatches(items []string, processBatch func([]string), batchSize int) (numChunks int) {
	// Get bath splits for provided items
	numFullChunks := len(items) / batchSize
	remainingSize := len(items) % batchSize
	numChunks = numFullChunks

	// process full batches
	for i := 0; i < numFullChunks; i++ {
		chunk := items[i*batchSize : (i+1)*batchSize]
		processBatch(chunk)
	}

	// process any remaining
	if remainingSize > 0 {
		numChunks = numFullChunks + 1
		lastChunk := items[numFullChunks*batchSize : (numFullChunks*batchSize + remainingSize)]
		processBatch(lastChunk)
	}
	return numChunks
}

// unique returns an array containing the unique elements of the provided array
func unique(duplicated []string) (unique []string) {
	return createArray(createMap(duplicated))
}

// createArray creates an array of keys from the provided map
func createArray(m map[string]struct{}) (a []string) {
	for k := range m {
		a = append(a, k)
	}
	return a
}

// createMap creates a map whose keys are the unique values of the provided array(s).
// values are empty structs for memory efficiency reasons (no storage used)
func createMap(a ...[]string) (m map[string]struct{}) {
	m = make(map[string]struct{})
	for _, aa := range a {
		for _, val := range aa {
			m[val] = struct{}{}
		}
	}
	return m
}

// statementSummary returns a summarized statement for logging, removing long lists of IDs or codes
func statementSummary(stmt string) string {
	if strings.HasPrefix(stmt, "g.V('") {
		i := strings.Index(stmt, "')")
		return "g.V(...)" + stmt[i+2:]
	}
	if i := strings.Index(stmt, "within(["); i != -1 {
		j := strings.Index(stmt[i:], "])")
		return stmt[:i] + "within([...])" + stmt[i+j+2:]
	}
	return stmt
}
