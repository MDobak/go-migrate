package migrate

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/fs"
	"path"
	"strconv"
	"strings"

	"github.com/mdobak/go-xerrors"
)

const sqlUpSeparator = "--UP--"
const sqlDownSeparator = "--DOWN--"
const sqlSnapshotSeparator = "--SNAPSHOT--"

type FS interface {
	fs.FS
	fs.ReadDirFS
	fs.ReadFileFS
}

type FilesystemProvider struct {
	fs   FS
	path string
}

func NewFilesystemProvider(fs FS, path string) *FilesystemProvider {
	return &FilesystemProvider{fs: fs, path: path}
}

func (p *FilesystemProvider) List(ctx context.Context) ([]Migration, error) {
	dir, err := p.fs.ReadDir(p.path)
	if err != nil {
		return nil, err
	}
	var ms []Migration
	for _, e := range dir {
		if e.IsDir() {
			continue
		}
		fp := path.Join(p.path, e.Name())
		v, ok := parseFileName(fp)
		if !ok {
			return nil, xerrors.New("invalid file name")
		}
		ms = append(ms, &fileMigration{fs: p.fs, filepath: fp, version: v})
	}
	return ms, nil
}

type fileMigration struct {
	fs       FS
	filepath string
	version  int
	isRead   bool

	up, down, snapshot []byte
}

func (m *fileMigration) Version(ctx context.Context) int {
	return m.version
}

func (m *fileMigration) Up(ctx context.Context) (string, error) {
	if err := m.read(); err != nil {
		return "", err
	}
	return string(m.up), nil
}

func (m *fileMigration) Down(ctx context.Context) (string, error) {
	if err := m.read(); err != nil {
		return "", err
	}
	return string(m.down), nil
}

func (m *fileMigration) Snapshot(ctx context.Context) (string, error) {
	if err := m.read(); err != nil {
		return "", err
	}
	return string(m.snapshot), nil
}

func (m *fileMigration) read() error {
	if m.isRead {
		return nil
	}
	f, err := m.fs.Open(m.filepath)
	if err != nil {
		return err
	}
	defer f.Close()
	m.up, m.down, m.snapshot = parseFileContent(f)
	m.isRead = true
	return nil
}

func parseFileContent(r io.Reader) ([]byte, []byte, []byte) {
	const (
		outside  = 0
		up       = 1
		down     = 2
		snapshot = 3
	)
	s := bufio.NewScanner(r)
	var loc = outside
	var bUp, bDown, bSnapshot bytes.Buffer
	for s.Scan() {
		line := s.Bytes()
		switch {
		case bytes.Equal(line, []byte(sqlUpSeparator)):
			loc = up
		case bytes.Equal(line, []byte(sqlDownSeparator)):
			loc = down
		case bytes.Equal(line, []byte(sqlSnapshotSeparator)):
			loc = snapshot
		case loc == up:
			bUp.Write(line)
			bUp.WriteByte('\n')
		case loc == down:
			bDown.Write(line)
			bDown.WriteByte('\n')
		case loc == snapshot:
			bSnapshot.Write(line)
			bSnapshot.WriteByte('\n')
		}
	}
	return bytes.TrimSpace(bUp.Bytes()), bytes.TrimSpace(bDown.Bytes()), bytes.TrimSpace(bSnapshot.Bytes())
}

func parseFileName(filepath string) (int, bool) {
	p := strings.Split(path.Base(filepath), ".")
	if len(p) != 2 || p[1] != "sql" {
		return 0, false
	}
	v, err := strconv.Atoi(strings.SplitN(p[0], "_", 2)[0])
	if err != nil {
		return 0, false
	}
	return v, true
}
