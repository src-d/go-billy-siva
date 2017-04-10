package sivafs

import (
	"io"
	"sync"

	"gopkg.in/src-d/go-billy.v2"
	"gopkg.in/src-d/go-siva.v1"
)

type file struct {
	billy.BaseFile

	w siva.Writer
	r *io.SectionReader
	l *sync.Mutex

	closeNotify func() error
}

func newFile(filename string, w siva.Writer,
	l *sync.Mutex, closeNotify func() error) billy.File {

	return &file{
		BaseFile:    billy.BaseFile{BaseFilename: filename},
		w:           w,
		l:           l,
		closeNotify: closeNotify,
	}
}

func openFile(filename string, r *io.SectionReader, l *sync.Mutex) billy.File {
	return &file{
		BaseFile: billy.BaseFile{BaseFilename: filename},
		r:        r,
		l:        l,
	}
}

func (f *file) Read(p []byte) (int, error) {
	if f.r == nil {
		return 0, ErrWriteOnlyFile
	}

	f.l.Lock()
	defer f.l.Unlock()

	return f.r.Read(p)
}

func (f *file) ReadAt(b []byte, off int64) (int, error) {
	if f.r == nil {
		return 0, ErrWriteOnlyFile
	}

	f.l.Lock()
	defer f.l.Unlock()

	return f.r.ReadAt(b, off)
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	if f.r == nil {
		return 0, ErrNonSeekableFile
	}

	if f.w != nil {
		panic("invalid state: both reader and writer are nil")
	}

	f.l.Lock()
	defer f.l.Unlock()

	return f.r.Seek(offset, whence)
}

func (f *file) Write(p []byte) (int, error) {
	if f.w == nil {
		return 0, ErrReadOnlyFile
	}

	f.l.Lock()
	defer f.l.Unlock()

	return f.w.Write(p)
}

func (f *file) Close() error {
	if f.Closed {
		return ErrAlreadyClosed
	}

	defer func() { f.Closed = true }()

	if f.closeNotify == nil {
		return nil
	}

	return f.closeNotify()
}
