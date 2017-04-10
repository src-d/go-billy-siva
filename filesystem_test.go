package sivafs

import (
	"io"
	"io/ioutil"
	stdos "os"
	"path/filepath"
	"testing"

	. "gopkg.in/check.v1"
	"gopkg.in/src-d/go-billy.v2/osfs"
)

func Test(t *testing.T) { TestingT(t) }

type SpecificFilesystemSuite struct {
	tmpDir string
}

var _ = Suite(&SpecificFilesystemSuite{})

type Fixture struct {
	name     string
	contents []string
}

const fixturesPath = "fixtures"

var fixtures = []*Fixture{
	{
		name: "basic.siva",
		contents: []string{
			"gopher.txt",
			"readme.txt",
			"todo.txt",
		},
	},
}

func (f *Fixture) Path() string {
	return filepath.Join(fixturesPath, f.name)
}

func (s *SpecificFilesystemSuite) SetUpTest(c *C) {
	s.tmpDir = c.MkDir()
}

func (s *SpecificFilesystemSuite) TestReadFs(c *C) {
	for _, fixture := range fixtures {
		s.testReadFs(c, fixture)
	}
}

func (s *SpecificFilesystemSuite) testReadFs(c *C, fixture *Fixture) {
	name := fixture.name
	err := copyFile(fixture.Path(), filepath.Join(s.tmpDir, name))
	c.Assert(err, IsNil)
	osFs := osfs.New(s.tmpDir)

	fs := New(osFs, name)
	c.Assert(fs, NotNil)

	c.Assert(fs.Base(), Equals, "/")

	for _, path := range fixture.contents {
		f, err := fs.Open(path)
		c.Assert(err, IsNil, Commentf("error opening %s", path))
		c.Assert(f, NotNil)
		read, err := ioutil.ReadAll(f)
		c.Assert(err, IsNil)
		c.Assert(len(read) > 0, Equals, true)
		err = f.Close()
		c.Assert(err, IsNil)
	}

	f, err := fs.Open("NON-EXISTANT")
	c.Assert(f, IsNil)
	c.Assert(err, Equals, stdos.ErrNotExist)

	for _, dir := range []string{"", ".", "/"} {
		dirLs, err := fs.ReadDir(dir)
		c.Assert(err, IsNil)
		c.Assert(len(dirLs), Equals, len(fixture.contents))
		// Here we assume that ReadDir returns contents in physical order.
		for idx, fi := range dirLs {
			c.Assert(fixture.contents[idx], Equals, fi.Name())
		}
	}

	dirLs, err := fs.ReadDir("NON-EXISTANT")
	c.Assert(err, IsNil)
	c.Assert(dirLs, HasLen, 0)

	for _, path := range fixture.contents {
		fi, err := fs.Stat(path)
		c.Assert(err, IsNil)
		c.Assert(fi.Name(), Equals, path)
	}

	fi, err := fs.Stat("NON-EXISTANT")
	c.Assert(fi, IsNil)
	c.Assert(err, Equals, stdos.ErrNotExist)

	subdirFs := fs.Dir("NON-EXISTANT")
	c.Assert(subdirFs, NotNil)
}

func copyFile(src, dst string) error {
	s, err := stdos.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()
	d, err := stdos.Create(dst)
	if err != nil {
		return err
	}
	defer d.Close()
	_, err = io.Copy(d, s)
	return err
}
