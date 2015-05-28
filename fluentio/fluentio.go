// Package fluentio wraps sequences of IO calls in a fluent transaction to simplify error handling
//
// Reduce repition and keep the flow of IO code clear by wrapping IO calls in a fluent
// "transaction".
package fluentio

import (
	"bufio"
	"io"
	"os"
)

// WriteFlusher adds the Flush() method to the io.Writer interface
type WriteFlusher interface {
	io.Writer
	Flush() error
}

// Writer is used to perform writes and other persistence functions on a file. This
// does NOT implement the io.Writer interface.
type Writer struct {
	err    error
	file   *os.File
	n      int
	writer WriteFlusher
}

// OpenFile opens the file and the fluent "transaction" by calling os.OpenFile() with the
// same parameters
func OpenFile(name string, flag int, perm os.FileMode) *Writer {

	fl := &Writer{}
	fl.file, fl.err = os.OpenFile(name, flag, perm)
	fl.writer = bufio.NewWriter(fl.file)
	return fl
}

// SetWriter allows the user to provide their own buffered writer.
//
// This is a bit of a dangerous method as it breaks encapsulation and opens things up to some nasty errors.
// If using this, make sure that w is constructed with this writer's file from GetFile() but don't just set
// it to the actual file. Undefined behaviour can occor in the former and double closes in the latter.
// There are no checks to ensure that these rules are followed. You've been warned.
func (fl *Writer) SetWriter(w WriteFlusher) *Writer {

	fl.writer = w
	return fl
}

// Write writes p to the opened file by calling os.Write on it. It is a NOP if a previous call generated an error
func (fl *Writer) Write(p []byte) *Writer {

	if fl.err != nil {
		return fl
	}
	fl.n, fl.err = fl.writer.Write(p)

	return fl
}

// Flush calls Flush on the write buffer. This function is a NOP if a previous call generated an error.
func (fl *Writer) Flush() *Writer {

	if fl.err != nil {
		return fl
	}
	fl.err = fl.writer.Flush()
	return fl
}

// Sync calls os.Sync on the opened file. This function is a NOP if enabled is false or if a previous call
// generated an error.
func (fl *Writer) Sync(enabled bool) *Writer {

	if !enabled {
		return fl
	}

	if fl.err != nil {
		return fl
	}
	fl.err = fl.file.Sync()
	return fl
}

// Close closes the underlying file.
//
// Closing the underlying file must always be permitted if it was successfully opened. If there is a pending
// error, that error is preserved and the Close error is thrown away. Close returns any pending errors.
// This means that Close must always be used last in the fluent call chain. No further calls can be made on
// the fluentIO object after Close.
func (fl *Writer) Close() error {

	if fl.file == nil {
		return nil
	}

	// We don't want the WriteFlusher interface to require the Closer interface
	// as many writers don't need it. However, when a writer does implement Closer
	// (e.g.gzip.Writer) then make sure it's called.
	//
	// Also make sure that fl.file and fl.writer are never the same object or you
	// will get a double close.
	if c, ok := fl.writer.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}
	fl.writer = nil

	err := fl.file.Close()
	fl.file = nil

	if err != nil && fl.err == nil {
		fl.err = err
	}
	return fl.err
}

// GetFile returns a pointer to the underlying os.File.
func (fl *Writer) GetFile() *os.File {
	return fl.file
}

// GetErr returns the underlying error object
func (fl *Writer) GetErr() error {
	return fl.err
}

// GetN returns the count of bytes that have been read or written to the file
func (fl *Writer) GetN() int {
	return fl.n
}

// Stat stats the opened file and passes the os.FileInfo to the callback. The callback
// can return with a non-nil to halt farther processing.
func (fl *Writer) Stat(cb func(fi os.FileInfo) error) *Writer {
	if fl.err != nil {
		return fl
	}

	var fi os.FileInfo
	fi, fl.err = fl.file.Stat()
	if fl.err != nil {
		return fl
	}

	fl.err = cb(fi)
	return fl
}
