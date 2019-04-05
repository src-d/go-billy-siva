package sivafs

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/helper/chroot"
	"gopkg.in/src-d/go-billy.v4/helper/mount"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-siva.v1"
)

var (
	ErrNonSeekableFile          = errors.New("file non-seekable")
	ErrFileWriteModeAlreadyOpen = errors.New("previous file in write mode already open")
	ErrReadOnlyFile             = errors.New("file is read-only")
	ErrWriteOnlyFile            = errors.New("file is write-only")
	ErrReadOnlyFilesystem       = errors.New("filesystem is read-only")
	ErrOffsetReadWrite          = errors.New("can only specify the offset in a read only filesystem")
)

const sivaCapabilities = billy.ReadCapability |
	billy.WriteCapability |
	billy.SeekCapability

type SivaSync interface {
	// Sync closes any open files, this method should be called at the end of
	// program to ensure that all the files are properly closed, otherwise the
	// siva file will be corrupted.
	Sync() error
}

type SivaBasicFS interface {
	billy.Basic
	billy.Dir

	SivaSync
}

type SivaFS interface {
	billy.Filesystem
	SivaSync
}

// SivaFSOptions holds configuration options for the filesystem.
type SivaFSOptions struct {
	// UnsafePaths set to on does not sanitize file paths.
	UnsafePaths bool
	// ReadOnly opens the siva file in read only mode.
	ReadOnly bool
	// Offset specifies the offset of the index. If it is 0 then the latest
	// index is used. This is only usable in read only mode.
	Offset uint64
}

type sivaFS struct {
	underlying billy.Filesystem
	path       string
	f          billy.File
	rw         *siva.ReadWriter
	r          siva.Reader

	fileWriteModeOpen bool
	options           SivaFSOptions
}

// New creates a new filesystem backed by a siva file with the given path in
// the given filesystem. The siva file will be opened or created lazily with
// the first operation.
//
// All files opened in write mode must be closed, otherwise the siva file will
// be corrupted.
func New(fs billy.Filesystem, path string) SivaBasicFS {
	return NewWithOptions(fs, path, SivaFSOptions{})
}

// NewWithOptions creates a new siva backed filesystem and accepts options.
// See New documentation.
func NewWithOptions(fs billy.Filesystem, path string, o SivaFSOptions) SivaBasicFS {
	return &sivaFS{
		underlying: fs,
		path:       path,
		options:    o,
	}
}

// NewFilesystem creates an entire filesystem using siva as the main backend,
// but supplying unsupported functionality using as a temporal files backend
// the main filesystem. It needs an additional parameter `tmpFs` where temporary
// files will be stored. Note that `tmpFs` will be mounted as /tmp.
func NewFilesystem(fs billy.Filesystem, path string, tmpFs billy.Filesystem) (SivaFS, error) {
	return NewFilesystemWithOptions(fs, path, tmpFs, SivaFSOptions{})
}

// NewFilesystemWithOptions creates an entire filesystem siva as the main
// backend. It accepts options. See NewFilesystem documentation.
func NewFilesystemWithOptions(
	fs billy.Filesystem,
	path string,
	tmpFs billy.Filesystem,
	o SivaFSOptions,
) (SivaFS, error) {
	tempdir := "/tmp"

	if !o.ReadOnly && o.Offset != 0 {
		return nil, ErrOffsetReadWrite
	}

	root := NewWithOptions(fs, path, o)

	if o.ReadOnly {
		ro := &readOnly{
			Filesystem: chroot.New(root, "/"),
			SivaSync:   root,
		}

		return ro, nil
	}

	m := mount.New(root, tempdir, tmpFs)

	t := &temp{
		defaultDir: tempdir,
		SivaSync:   root,
		Filesystem: chroot.New(m, "/"),
	}

	return t, nil
}

// NewFilesystemReadOnly creates a read only filesystem backed by a siva file.
// offset is the index offset inside the siva file. Set it to 0 to use the
// last index.
func NewFilesystemReadOnly(
	fs billy.Filesystem,
	path string,
	offset uint64,
) (SivaFS, error) {
	return NewFilesystemWithOptions(fs, path, nil, SivaFSOptions{
		ReadOnly: true,
		Offset:   offset,
	})
}

// Create creates a new file. This file is created using CREATE, TRUNCATE and
// WRITE ONLY flags due to limitations working on siva files.
func (fs *sivaFS) Create(path string) (billy.File, error) {
	return fs.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(0666))
}

func (fs *sivaFS) Open(path string) (billy.File, error) {
	return fs.OpenFile(path, os.O_RDONLY, 0)
}

func (fs *sivaFS) OpenFile(path string, flag int, mode os.FileMode) (billy.File, error) {
	if err := fs.ensureOpen(); err != nil {
		return nil, err
	}

	if fs.rw == nil && flag&(os.O_CREATE|os.O_TRUNC|os.O_WRONLY) != 0 {
		return nil, ErrReadOnlyFilesystem
	}

	path = normalizePath(path)
	if flag&os.O_CREATE != 0 && flag&os.O_TRUNC == 0 {
		return nil, billy.ErrNotSupported
	}

	if flag&os.O_CREATE != 0 {
		if fs.fileWriteModeOpen {
			return nil, ErrFileWriteModeAlreadyOpen
		}

		return fs.createFile(path, flag, mode)
	}

	return fs.openFile(path, flag, mode)
}

func (fs *sivaFS) Stat(p string) (os.FileInfo, error) {
	p = normalizePath(p)

	if err := fs.ensureOpen(); err != nil {
		return nil, err
	}

	index, err := fs.getIndex()
	if err != nil {
		return nil, err
	}

	e := index.Find(p)
	if e != nil {
		return newFileInfo(e), nil
	}

	stat, err := getDir(index, p)
	if err != nil {
		return nil, err
	}

	if stat == nil {
		return nil, os.ErrNotExist
	}

	return stat, nil
}

func (fs *sivaFS) ReadDir(path string) ([]os.FileInfo, error) {
	path = normalizePath(path)

	if err := fs.ensureOpen(); err != nil {
		return nil, err
	}

	index, err := fs.getIndex()
	if err != nil {
		return nil, err
	}

	files, err := listFiles(index, path)
	if err != nil {
		return nil, err
	}

	dirs, err := listDirs(index, path)
	if err != nil {
		return nil, err
	}

	return append(dirs, files...), nil
}

func (fs *sivaFS) MkdirAll(filename string, perm os.FileMode) error {
	filename = normalizePath(filename)

	if err := fs.ensureOpen(); err != nil {
		return err
	}

	if fs.rw == nil {
		return ErrReadOnlyFilesystem
	}

	index, err := fs.getIndex()
	if err != nil {
		return err
	}
	e := index.Find(filename)
	if e != nil {
		return &os.PathError{
			Op:   "mkdir",
			Path: filename,
			Err:  syscall.ENOTDIR,
		}
	}

	return nil
}

// Join joins the specified elements using the filesystem separator.
func (fs *sivaFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *sivaFS) Remove(path string) error {
	path = normalizePath(path)

	if err := fs.ensureOpen(); err != nil {
		return err
	}

	if fs.rw == nil {
		return ErrReadOnlyFilesystem
	}

	index, err := fs.getIndex()
	if err != nil {
		return err
	}

	e := index.Find(path)

	if e != nil {
		return fs.rw.WriteHeader(&siva.Header{
			Name:    path,
			ModTime: time.Now(),
			Mode:    0,
			Flags:   siva.FlagDeleted,
		})
	}

	dir, err := getDir(index, path)
	if err != nil {
		return err
	}

	if dir != nil {
		return &os.PathError{
			Op:   "remove",
			Path: path,
			Err:  syscall.ENOTEMPTY,
		}
	}

	// there are no file and no directory with this path
	return os.ErrNotExist
}

func (fs *sivaFS) Rename(from, to string) error {
	return billy.ErrNotSupported
}

func (fs *sivaFS) Sync() error {
	return fs.ensureClosed()
}

// Capability implements billy.Capable interface.
func (fs *sivaFS) Capabilities() billy.Capability {
	return sivaCapabilities
}

func (fs *sivaFS) ensureOpen() error {
	if fs.r != nil {
		return nil
	}

	if fs.options.ReadOnly {
		f, err := fs.underlying.Open(fs.path)
		if err != nil {
			return err
		}

		r := siva.NewReaderWithOffset(f, fs.options.Offset)

		fs.r = r
		fs.f = f
		return nil
	}

	f, err := fs.underlying.OpenFile(fs.path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	rw, err := siva.NewReaderWriter(f)
	if err != nil {
		f.Close()
		return err
	}

	fs.rw = rw
	fs.r = rw
	fs.f = f
	return nil
}

func (fs *sivaFS) ensureClosed() error {
	if fs.r == nil {
		return nil
	}

	if fs.rw != nil {
		if err := fs.rw.Close(); err != nil {
			return err
		}
	}

	fs.rw = nil
	fs.r = nil

	f := fs.f
	fs.f = nil
	return f.Close()
}

func (fs *sivaFS) createFile(path string, flag int, mode os.FileMode) (billy.File, error) {
	if flag&os.O_RDWR != 0 || flag&os.O_RDONLY != 0 {
		return nil, billy.ErrNotSupported
	}

	header := &siva.Header{
		Name:    path,
		Mode:    mode,
		ModTime: time.Now(),
	}

	if err := fs.rw.WriteHeader(header); err != nil {
		return nil, err
	}

	closeFunc := func() error {
		if fs.rw == nil {
			return nil
		}

		if flag&os.O_WRONLY != 0 || flag&os.O_RDWR != 0 {
			fs.fileWriteModeOpen = false
		}

		return fs.rw.Flush()
	}

	defer func() { fs.fileWriteModeOpen = true }()
	return newFile(path, fs.rw, closeFunc), nil
}

func (fs *sivaFS) openFile(path string, flag int, mode os.FileMode) (billy.File, error) {
	if flag&os.O_RDWR != 0 || flag&os.O_WRONLY != 0 {
		return nil, billy.ErrNotSupported
	}

	index, err := fs.getIndex()
	if err != nil {
		return nil, err
	}

	e := index.Find(path)
	if e == nil {
		return nil, os.ErrNotExist
	}

	sr, err := fs.r.Get(e)
	if err != nil {
		return nil, err
	}

	return openFile(path, sr), nil
}

func (fs *sivaFS) getIndex() (siva.OrderedIndex, error) {
	index, err := fs.r.Index()
	if err != nil {
		return nil, err
	}

	if fs.options.UnsafePaths {
		return siva.OrderedIndex(index), nil
	}

	return siva.OrderedIndex(index.ToSafePaths()), nil
}

func listFiles(index siva.OrderedIndex, dir string) ([]os.FileInfo, error) {
	dir = addTrailingSlash(dir)

	entries, err := siva.Index(index).Glob(fmt.Sprintf("%s*", dir))
	if err != nil {
		return nil, err
	}

	contents := []os.FileInfo{}
	for _, e := range entries {
		contents = append(contents, newFileInfo(e))
	}

	return contents, nil
}

func getDir(index siva.OrderedIndex, dir string) (os.FileInfo, error) {
	dir = addTrailingSlash(dir)
	lenDir := len(dir)

	entries := make([]*siva.IndexEntry, 0)

	for _, e := range index {
		if len(e.Name) > lenDir {
			if e.Name[:lenDir] == dir {
				entries = append(entries, e)
			}
		}
	}

	if len(entries) == 0 {
		return nil, nil
	}

	var oldDir time.Time
	for _, e := range entries {
		if oldDir.Before(e.ModTime) {
			oldDir = e.ModTime
		}
	}

	return newDirFileInfo(path.Clean(dir), oldDir), nil
}

func listDirs(index siva.OrderedIndex, dir string) ([]os.FileInfo, error) {
	dir = addTrailingSlash(dir)

	depth := strings.Count(dir, "/")
	dirs := map[string]time.Time{}
	dirOrder := make([]string, 0)
	for _, e := range index {
		if !strings.HasPrefix(e.Name, dir) {
			continue
		}

		targetParts := strings.Split(e.Name, "/")
		if len(targetParts) <= depth+1 {
			continue
		}

		dir := strings.Join(targetParts[:depth+1], "/")
		oldDir, ok := dirs[dir]
		if !ok || oldDir.Before(e.ModTime) {
			dirs[dir] = e.ModTime
			if !ok {
				dirOrder = append(dirOrder, dir)
			}
		}
	}

	contents := []os.FileInfo{}
	for _, dir := range dirOrder {
		contents = append(contents, newDirFileInfo(dir, dirs[dir]))
	}

	return contents, nil
}

// addTrailingSlash adds trailing slash to the path if it does not have one.
func addTrailingSlash(path string) string {
	if path == "" {
		return path
	}

	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}

	return path
}

// normalizePath returns a path relative to '/'.
// It assumes UNIX-style slash-delimited paths.
func normalizePath(path string) string {
	path = filepath.Join(string(filepath.Separator), path)
	path = filepath.ToSlash(path)
	return removeLeadingSlash(path)
}

// removeLeadingSlash removes leading slash of the path, if any.
func removeLeadingSlash(path string) string {
	if strings.HasPrefix(path, "/") {
		return path[1:]
	}

	return path
}

type temp struct {
	billy.Filesystem
	SivaSync

	defaultDir string
}

// Capability implements billy.Capable interface.
func (h *temp) Capabilities() billy.Capability {
	return sivaCapabilities
}

// Capability implements billy.TempFile interface.
func (h *temp) TempFile(dir, prefix string) (billy.File, error) {
	dir = h.Join(h.defaultDir, dir)

	return util.TempFile(h.Filesystem, dir, prefix)
}

type readOnly struct {
	billy.Filesystem
	SivaSync
}

// Capability implements billy.Capable interface.
func (r *readOnly) Capabilities() billy.Capability {
	return sivaCapabilities & ^billy.WriteCapability
}

// Capability implements billy.TempFile interface.
func (r *readOnly) TempFile(dir, prefix string) (billy.File, error) {
	return nil, ErrReadOnlyFilesystem
}
