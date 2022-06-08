package migrate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type databaseMock struct {
	mock.Mock
}

func (d *databaseMock) List(_ context.Context) ([]int, error) {
	args := d.Called()
	return args.Get(0).([]int), args.Error(1)
}

func (d *databaseMock) Migrate(_ context.Context, actions []Action) error {
	args := d.Called(actions)
	return args.Error(0)
}

type providerMock struct {
	mock.Mock
}

func (p *providerMock) List(_ context.Context) ([]Migration, error) {
	args := p.Called()
	return args.Get(0).([]Migration), args.Error(1)
}

type testMigration struct {
	version  int
	up       string
	down     string
	snapshot string
}

func (m *testMigration) Version(_ context.Context) int {
	return m.version
}

func (m *testMigration) Up(_ context.Context) (string, error) {
	return m.up, nil
}

func (m *testMigration) Down(_ context.Context) (string, error) {
	return m.down, nil
}

func (m *testMigration) Snapshot(_ context.Context) (string, error) {
	return m.snapshot, nil
}

func TestMigrate_Plan_Up(t *testing.T) {
	ctx := context.Background()

	p := &providerMock{}
	d := &databaseMock{}
	m := New(p, d)

	var migrations []Migration
	migrations = append(migrations,
		&testMigration{version: 1, up: "up1", down: "down1"},
		&testMigration{version: 2, up: "up2", down: "down2"},
	)

	t.Run("up-to-1", func(t *testing.T) {
		p.On("List").Return(migrations, nil)
		d.On("List").Return([]int{}, nil)
		actions, err := m.Plan(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, []Action{
			{Version: 1, Direction: Up, Migration: "up1"},
		}, actions)
	})
	t.Run("up-to-2", func(t *testing.T) {
		p.On("List").Return(migrations, nil)
		d.On("List").Return([]int{}, nil)
		actions, err := m.Plan(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, []Action{
			{Version: 1, Direction: Up, Migration: "up1"},
			{Version: 2, Direction: Up, Migration: "up2"},
		}, actions)
	})
	t.Run("up-to-3", func(t *testing.T) {
		p.On("List").Return(migrations, nil)
		d.On("List").Return([]int{}, nil)
		actions, err := m.Plan(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, []Action{
			{Version: 1, Direction: Up, Migration: "up1"},
			{Version: 2, Direction: Up, Migration: "up2"},
		}, actions)
	})
}

func TestMigrate_Plan_Down(t *testing.T) {
	ctx := context.Background()

	p := &providerMock{}
	d := &databaseMock{}
	m := New(p, d)

	var migrations []Migration
	migrations = append(migrations,
		&testMigration{version: 1, up: "up1", down: "down1"},
		&testMigration{version: 2, up: "up2", down: "down2"},
		&testMigration{version: 3, up: "up3", down: "down3"},
	)
	appliedMigrations := []int{1, 2, 3}

	t.Run("down-to-2", func(t *testing.T) {
		p.On("List").Return(migrations, nil)
		d.On("List").Return(appliedMigrations, nil)
		actions, err := m.Plan(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, []Action{
			{Version: 3, Direction: Down, Migration: "down3"},
		}, actions)
	})
	t.Run("down-to-1", func(t *testing.T) {
		p.On("List").Return(migrations, nil)
		d.On("List").Return(appliedMigrations, nil)
		actions, err := m.Plan(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, []Action{
			{Version: 3, Direction: Down, Migration: "down3"},
			{Version: 2, Direction: Down, Migration: "down2"},
		}, actions)
	})
	t.Run("down-to-0", func(t *testing.T) {
		p.On("List").Return(migrations, nil)
		d.On("List").Return(appliedMigrations, nil)
		actions, err := m.Plan(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, []Action{
			{Version: 3, Direction: Down, Migration: "down3"},
			{Version: 2, Direction: Down, Migration: "down2"},
			{Version: 1, Direction: Down, Migration: "down1"},
		}, actions)
	})
}

func TestMigrate_Plan_Up_SkipGaps(t *testing.T) {
	ctx := context.Background()

	p := &providerMock{}
	d := &databaseMock{}
	m := New(p, d)

	var migrations []Migration
	migrations = append(migrations,
		&testMigration{version: 1, up: "up1", down: "down1"},
		&testMigration{version: 2, up: "up2", down: "down2"},
		&testMigration{version: 3, up: "up3", down: "down3"},
		&testMigration{version: 4, up: "up4", down: "down4"},
	)
	appliedMigrations := []int{1, 3}

	p.On("List").Return(migrations, nil)
	d.On("List").Return(appliedMigrations, nil)
	actions, err := m.Plan(ctx, 4)
	require.NoError(t, err)

	// Version 2 is not applied but 3 is. It means that there is some problem
	// with migrations, probably migration with a lower version number was
	// added after a migration with a higher version number was already applied.
	// In that case applying migration 2 out of order may be dangerous, so we
	// will skip it.
	require.Equal(t, []Action{
		{Version: 4, Direction: Up, Migration: "up4"},
	}, actions)
}

func TestMigrate_Plan_Up_SkipToSnapshot(t *testing.T) {
	ctx := context.Background()

	p := &providerMock{}
	d := &databaseMock{}
	m := New(p, d)

	var migrations []Migration
	migrations = append(migrations,
		&testMigration{version: 1, up: "up1", down: "down1"},
		&testMigration{version: 2, up: "up2", down: "down2", snapshot: "snapshot2"},
		&testMigration{version: 3, up: "up3", down: "down3"},
		&testMigration{version: 4, up: "up4", down: "down4", snapshot: "snapshot4"},
		&testMigration{version: 5, up: "up5", down: "down5"},
	)

	p.On("List").Return(migrations, nil)
	d.On("List").Return([]int{}, nil)
	actions, err := m.Plan(ctx, 5)
	require.NoError(t, err)

	require.Equal(t, []Action{
		{Version: 4, Direction: Up, Migration: "snapshot4"},
		{Version: 5, Direction: Up, Migration: "up5"},
	}, actions)
}
