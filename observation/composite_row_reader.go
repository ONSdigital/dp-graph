package observation

import (
	"context"
	"io"
)

var _ StreamRowReader = (*CompositeRowReader)(nil)

// CompositeRowReader abstracts multiple StreamRowReader's to act as one
type CompositeRowReader struct {
	readers     []StreamRowReader
	readerIndex int
}

func NewCompositeRowReader(readers ...StreamRowReader) *CompositeRowReader {
	return &CompositeRowReader{
		readers:     readers,
		readerIndex: 0,
	}
}

func (c *CompositeRowReader) Read() (row string, err error) {

	// attempt to read from the current reader
	row, err = c.readers[c.readerIndex].Read()

	// if the current reader is EOF, move to the next reader
	if err == io.EOF {
		c.readerIndex++

		// if there is no more readers, return EOF
		if c.readerIndex == len(c.readers) {
			return "", io.EOF
		}

		// recursive call to read from the next reader
		return c.Read()
	} else if err != nil {
		return "", err
	}

	return row, nil
}

func (c *CompositeRowReader) Close(ctx context.Context) error {
	for i := range c.readers {
		err := c.readers[i].Close(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
