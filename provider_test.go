package migrate

import (
	"context"
	"embed"
	"testing"

	"github.com/stretchr/testify/require"
)

//go:embed testdata
var testdata embed.FS

const sqlUp1 = `
CREATE TABLE test
(
    id   bigint    NOT NULL,
    time timestamp NOT NULL,
    PRIMARY KEY (id)
);`

const sqlDown1 = `
DROP TABLE test;`

const sqlUp2 = `
CREATE TABLE test2
(
    id   bigint    NOT NULL,
    time timestamp NOT NULL,
    PRIMARY KEY (id)
);`

const sqlDown2 = `
DROP TABLE test2;`

const sqlSnapshot2 = `
CREATE TABLE test
(
    id   bigint    NOT NULL,
    time timestamp NOT NULL,
    PRIMARY KEY (id)
);
CREATE TABLE test2
(
    id   bigint    NOT NULL,
    time timestamp NOT NULL,
    PRIMARY KEY (id)
);`

func TestFilesystemProvider_List(t *testing.T) {
	ctx := context.Background()

	p := NewFilesystemProvider(testdata, "testdata")
	ms, err := p.List(ctx)

	require.NoError(t, err)
	require.Len(t, ms, 2)

	// 1
	up1, err := ms[0].Up(ctx)
	require.NoError(t, err)
	down1, err := ms[0].Down(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, ms[0].Version(ctx))
	require.Equal(t, sqlUp1[1:], up1)
	require.Equal(t, sqlDown1[1:], down1)

	// 2
	up2, err := ms[1].Up(ctx)
	require.NoError(t, err)
	down2, err := ms[1].Down(ctx)
	require.NoError(t, err)
	snapshot2, err := ms[1].Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, ms[1].Version(ctx))
	require.Equal(t, sqlUp2[1:], up2)
	require.Equal(t, sqlDown2[1:], down2)
	require.Equal(t, sqlSnapshot2[1:], snapshot2)
}
