package neptune

import (
	"strings"
	"sync"
)

// batchProcessor defines a generic function type to process a batch (array of strings) and may return a result (array of strings) and an error.
type batchProcessor = func(map[string]interface{}) (map[string]interface{}, error)

// processInConcurrentBatches splits the provided items in batches and calls processBatch for each batch batch, concurrently.
// The results of the batch Processor functions, if provided, are aggregated as unique items and returned.
// note that the items are not processed in any deterministic order
func processInConcurrentBatches(items map[string]interface{}, processBatch batchProcessor, batchSize, maxWorkers int) (result map[string]interface{}, numChunks int, errs []error) {
	wg := sync.WaitGroup{}
	chWait := make(chan struct{})
	chErr := make(chan error)
	chSemaphore := make(chan struct{}, maxWorkers)

	result = make(map[string]interface{})
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
	doProcessBatch := func(chunk map[string]interface{}) {
		defer release()
		res, err := processBatch(chunk)
		if err != nil {
			chErr <- err
			return
		}
		lockResult.Lock()
		for k, v := range res {
			result[k] = v
		}
		lockResult.Unlock()
	}

	// func that triggers the batch processing for a chunk, in a parallel go-routine
	goProcessBatch := func(chunk map[string]interface{}) {
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
// note that the items are not processed in any deterministic order
func processInBatches(items map[string]interface{}, processBatch func(map[string]interface{}), batchSize int) (numChunks int) {
	numChunks = 0

	// process full bathes, reseting the batch at the end of each process
	batch := make(map[string]interface{}, batchSize)
	for k, v := range items {
		batch[k] = v
		if len(batch) == batchSize {
			numChunks++
			processBatch(batch)
			batch = make(map[string]interface{}, batchSize)
		}
	}

	// process any remaining items
	if len(batch) > 0 {
		processBatch(batch)
		numChunks++
	}
	return numChunks
}

// unique returns an array containing the unique elements of the provided array
func unique(duplicated []string) (unique []string) {
	return createArray(createInterfaceMapFromArrays(duplicated))
}

// createArray creates an array of keys from the provided map
func createArray(m map[string]interface{}) (a []string) {
	for k := range m {
		a = append(a, k)
	}
	return a
}

// createInterfaceMapFromArrays creates a map whose keys are the unique values of the provided array(s).
// values are empty structs for memory efficiency reasons (no storage used)
func createInterfaceMapFromArrays(a ...[]string) (m map[string]interface{}) {
	m = make(map[string]interface{})
	for _, aa := range a {
		for _, val := range aa {
			m[val] = struct{}{}
		}
	}
	return m
}

// createMap creates a map whose keys are the unique values of the provided array(s).
// values are empty strings
func createStringMapFromArrays(a ...[]string) (m map[string]string) {
	m = make(map[string]string)
	for _, aa := range a {
		for _, val := range aa {
			m[val] = ""
		}
	}
	return m
}

func createStringMap(in map[string]interface{}) (out map[string]string) {
	out = make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v.(string)
	}
	return out
}

func createInterfaceMap(in map[string]string) (out map[string]interface{}) {
	out = make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
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
