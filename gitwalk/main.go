package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/lmittmann/tint"
	"github.com/xdb-dev/xdb"
	"github.com/xdb-dev/xdb/schema"
	xdbsqlite "github.com/xdb-dev/xdb/stores/sqlite"
	"zombiezen.com/go/sqlite"
)

func main() {
	logger := slog.New(tint.NewHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		slog.Error("Usage: gitwalk <path>")
		os.Exit(1)
	}

	root := os.Args[1]
	ctx := context.Background()

	schema := &schema.Schema{
		Records: []schema.Record{
			{
				Kind:  "Commit",
				Table: "commits",
				Attributes: []schema.Attribute{
					{Name: "created_at", Type: schema.Time},
					{Name: "email", Type: schema.String},
					{Name: "name", Type: schema.String},
					{Name: "message", Type: schema.String},
					{Name: "repo", Type: schema.String},
					{Name: "branch", Type: schema.String},
				},
			},
		},
	}

	db, err := sqlite.OpenConn("gitwalk.db", sqlite.OpenReadWrite|sqlite.OpenCreate)
	if err != nil {
		panic(err)
	}

	defer db.Close()

	m := xdbsqlite.NewMigration(db, schema)

	err = m.Run(ctx)
	if err != nil {
		panic(err)
	}

	store := xdbsqlite.NewSQLiteStore(db, schema)

	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			r, err := git.PlainOpen(path)
			if err != nil {
				return nil
			}

			// read all branches
			branches, err := r.Branches()
			if err != nil {
				return err
			}

			err = branches.ForEach(func(b *plumbing.Reference) error {
				// read all commits for each branch
				cIter, err := r.Log(&git.LogOptions{From: b.Hash()})
				if err != nil {
					return err
				}

				return cIter.ForEach(func(c *object.Commit) error {
					return saveCommit(ctx, store, r, b, c)
				})
			})
			if err != nil {
				return err
			}

			slog.Info("Saved commits", "repo", path)

			return filepath.SkipDir
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	// iterate over all repos

}

func saveCommit(ctx context.Context, store *xdbsqlite.SQLiteStore, r *git.Repository, b *plumbing.Reference, c *object.Commit) error {
	wt, _ := r.Worktree()
	repoPath := wt.Filesystem.Root()

	commitKey := xdb.NewKey("Commit", c.Hash.String())
	commitRecord := xdb.NewRecord(commitKey,
		xdb.NewTuple(commitKey, "created_at", c.Author.When),
		xdb.NewTuple(commitKey, "email", c.Author.Email),
		xdb.NewTuple(commitKey, "name", c.Author.Name),
		xdb.NewTuple(commitKey, "message", c.Message),
		xdb.NewTuple(commitKey, "repo", repoPath),
		xdb.NewTuple(commitKey, "branch", b.Name().Short()),
	)

	err := store.PutRecord(ctx, commitRecord)
	if err != nil {
		return err
	}

	return nil
}
