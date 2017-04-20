package sivafs

import (
	"io"

	"gopkg.in/src-d/go-billy.v2"
	"gopkg.in/src-d/go-siva.v1"
)

type file struct {
	billy.BaseFile

	w siva.Writer
	r *io.SectionReader

	closeNotify func() error
}

func newFile(filename string, w siva.Writer, closeNotify func() error) billy.File {

	return &file{
		BaseFile:    billy.BaseFile{BaseFilename: filename},
		w:           w,
		closeNotify: closeNotify,
	}
}

func openFile(filename string, r *io.SectionReader) billy.File {
	return &file{
		BaseFile: billy.BaseFile{BaseFilename: filename},
		r:        r,
	}
}

func (f *file) Read(p []byte) (int, error) {
	if f.Closed {
		return 0, ErrAlreadyClosed
	}

	if f.r == nil {
		return 0, ErrWriteOnlyFile
	}

	return f.r.Read(p)
}

func (f *file) ReadAt(b []byte, off int64) (int, error) {
	if f.Closed {
		return 0, ErrAlreadyClosed
	}

	if f.r == nil {
		return 0, ErrWriteOnlyFile
	}

	return f.r.ReadAt(b, off)
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	if f.Closed {
		return 0, ErrAlreadyClosed
	}

	if f.r == nil {
		return 0, ErrNonSeekableFile
	}

	return f.r.Seek(offset, whence)
}

func (f *file) Write(p []byte) (int, error) {
	if f.Closed {
		return 0, ErrAlreadyClosed
	}

	if f.w == nil {
		return 0, ErrReadOnlyFile
	}

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
