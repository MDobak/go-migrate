package go_migrate

import (
	"context"
	"fmt"
	"sort"

	"github.com/mdobak/go-xerrors"
)

var ErrMigrator = xerrors.Message("migrator")

// Provider provides list of available migrations.
type Provider interface {
	List(ctx context.Context) ([]Migration, error)
}

// Database is the interface for the database on which the migrations will
// be executed.
type Database interface {
	List(ctx context.Context) ([]int, error)
	Migrate(ctx context.Context, actions []Action) error
}

// Direction is a migration direction.
type Direction uint8

const (
	Up   Direction = 1
	Down Direction = 2
)

// Migration represent a single migration that can be executed.
type Migration interface {
	Version(ctx context.Context) int
	Up(ctx context.Context) (string, error)
	Down(ctx context.Context) (string, error)
	Snapshot(ctx context.Context) (string, error)
}

type Action struct {
	Version   int
	Migration string
	Direction Direction
}

type Migrator struct {
	prov Provider
	db   Database
}

func New(prov Provider, db Database) *Migrator {
	return &Migrator{prov: prov, db: db}
}

// LatestVersion returns latest available migration version.
func (m *Migrator) LatestVersion(ctx context.Context) (int, error) {
	list, err := m.prov.List(ctx)
	if err != nil {
		return 0, xerrors.New(ErrMigrator, "unable to find latest version", err)
	}
	latest := 0
	for _, m := range list {
		if m.Version(ctx) > latest {
			latest = m.Version(ctx)
		}
	}
	return latest, nil
}

// CurrentVersion returns latest applied migration version.
func (m *Migrator) CurrentVersion(ctx context.Context) (int, error) {
	list, err := m.db.List(ctx)
	if err != nil {
		return 0, xerrors.New(ErrMigrator, "unable to find current version", err)
	}
	latest := 0
	for _, v := range list {
		if v > latest {
			latest = v
		}
	}
	return latest, nil
}

// Migrate executes up or down migrations needed to reach the required version.
// If version number is higher than the latest available version, then the
// latest version is used.
func (m *Migrator) Migrate(ctx context.Context, version int) error {
	actions, err := m.plan(ctx, version)
	if err != nil {
		return xerrors.New(ErrMigrator, "unable to prepare migration plan", err)
	}
	err = m.db.Migrate(ctx, actions)
	if err != nil {
		return xerrors.New(ErrMigrator, "unable to apply migrations", err)
	}
	return nil
}

// Plan returns a list migrations needed to reach the required version.
// If version number is higher than the latest available version, then
// the latest version is used.
func (m *Migrator) Plan(ctx context.Context, version int) ([]Action, error) {
	a, err := m.plan(ctx, version)
	if err != nil {
		return nil, xerrors.New(ErrMigrator, "unable to prepare migration plan", err)
	}
	return a, nil
}

func (m *Migrator) plan(ctx context.Context, version int) ([]Action, error) {
	// Fetch the list of available migrations from the provider.
	provList, err := m.prov.List(ctx)
	if err != nil {
		return nil, err
	}
	if len(provList) == 0 {
		// If provider did not send any migrations, we can stop here because
		// without migrations we cannot do anything anyway.
		return nil, nil
	}
	sort.Slice(provList, func(i, j int) bool {
		return provList[i].Version(ctx) < provList[j].Version(ctx)
	})

	// Fetch the list of applied migrations from the database.
	dbList, err := m.db.List(ctx)
	if err != nil {
		return nil, err
	}
	sort.Ints(dbList)

	// Find the latest applied migration.
	latest := 0
	if len(dbList) > 0 {
		latest = dbList[len(dbList)-1]
	}

	var actions []Action

	// Migrate up.
	if version > latest {
		for _, pm := range provList {
			if pm.Version(ctx) > version {
				break
			}
			// Check if migration is not already applied.
			idx := sort.SearchInts(dbList, pm.Version(ctx))
			if idx >= len(dbList) || dbList[idx] != pm.Version(ctx) {
				// If there is no migrations applied, we can try to use
				// a snapshot if available to speed up the process.
				if len(dbList) == 0 {
					snapshot, err := pm.Snapshot(ctx)
					if err != nil {
						return nil, xerrors.New(
							fmt.Sprintf("unable to load snapshot from migration %d", pm.Version(ctx)),
							err,
						)
					}
					if len(snapshot) > 0 {
						actions = []Action{{
							Version:   pm.Version(ctx),
							Migration: snapshot,
							Direction: Up,
						}}
						continue
					}
				}
				// Add an "up" migration.
				up, err := pm.Up(ctx)
				if err != nil {
					return nil, xerrors.New(
						fmt.Sprintf("unable to load %d up migration", pm.Version(ctx)),
						err,
					)
				}
				actions = append(actions, Action{
					Version:   pm.Version(ctx),
					Migration: up,
					Direction: Up,
				})
			} else {
				// To prevent a migration from being applied when a migration
				// with a higher version number has already been applied.
				actions = actions[0:0]
			}
		}
	}

	// Migrate down.
	if version < latest {
		for i := len(dbList) - 1; i >= 0; i-- {
			v := dbList[i]
			if v <= version {
				break
			}
			idx := sort.Search(len(provList), func(i int) bool {
				return provList[i].Version(ctx) >= v
			})
			if idx < len(provList) && provList[idx].Version(ctx) == v {
				down, err := provList[idx].Down(ctx)
				if err != nil {
					return nil, xerrors.New(
						fmt.Sprintf("unable to load %d down migration", provList[idx].Version(ctx)),
						err,
					)
				}
				actions = append(actions, Action{
					Version:   provList[idx].Version(ctx),
					Migration: down,
					Direction: Down,
				})
			}
		}
	}

	return actions, nil
}
