package sivafs

import (
	"io/ioutil"
	stdos "os"

	. "gopkg.in/check.v1"
	"gopkg.in/src-d/go-billy.v2"
	"gopkg.in/src-d/go-billy.v2/osfs"
	"gopkg.in/src-d/go-billy.v2/test"
)

type FilesystemSuite struct {
	test.FilesystemSuite
	cfs  billy.Filesystem
	path string
}

var _ = Suite(&FilesystemSuite{})

func (s *FilesystemSuite) SetUpTest(c *C) {
	s.path, _ = ioutil.TempDir(stdos.TempDir(), "go-git-fs-test")
	osFs := osfs.New(s.path)
	f, err := osFs.TempFile("", "siva-fs")
	c.Assert(err, IsNil)
	name := f.Filename()
	c.Assert(f.Close(), IsNil)
	fs := New(osFs, name)
	s.cfs = fs
	s.FilesystemSuite.Fs = fs
}

func (s *FilesystemSuite) TestTempFile(c *C) {
	c.Skip("TempFile not supported")
}

func (s *FilesystemSuite) TestTempFileFullWithPath(c *C) {
	c.Skip("TempFile not supported")
}

func (s *FilesystemSuite) TestTempFileWithPath(c *C) {
	c.Skip("TempFile not supported")
}

func (s *FilesystemSuite) TestRemoveTempFile(c *C) {
	c.Skip("TempFile not supported")
}

func (s *FilesystemSuite) TestRename(c *C) {
	c.Skip("Rename not supported")
}

func (s *FilesystemSuite) TestOpenFileAppend(c *C) {
	c.Skip("O_APPEND not supported")
}

func (s *FilesystemSuite) TestOpenFileNoTruncate(c *C) {
	c.Skip("O_CREATE without O_TRUNC not supported")
}

func (s *FilesystemSuite) TestOpenFileReadWrite(c *C) {
	c.Skip("O_RDWR not supported")
}

func (s *FilesystemSuite) TestFileCreateReadSeek(c *C) {
	c.Skip("does not support seek on writeable files")
}

func (s *FilesystemSuite) TestReadAtOnReadWrite(c *C) {
	c.Skip("ReadAt not supported on writeable files")
}

func (s *FilesystemSuite) TestMkdirAll(c *C) {
	c.Skip("MkdirAll method does nothing")
}

func (s *FilesystemSuite) TestMkdirAllIdempotent(c *C) {
	c.Skip("MkdirAll method does nothing")
}

func (s *FilesystemSuite) TestMkdirAllNested(c *C) {
	c.Skip("because MkdirAll does nothing, is not possible to check the " +
		"Stat of a directory created with this mehtod")
}
