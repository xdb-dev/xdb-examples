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
	"github.com/xdb-dev/xdb/stores/kv/memory"
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

	store := memory.NewStore()

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			r, err := git.PlainOpen(path)
			if err != nil {
				return nil
			}

			if err := saveRepo(ctx, store, r); err != nil {
				return err
			}

			// read all branches
			branches, err := r.Branches()
			if err != nil {
				return err
			}

			err = branches.ForEach(func(b *plumbing.Reference) error {
				if err := saveBranch(ctx, store, r, b); err != nil {
					return err
				}

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

			return filepath.SkipDir
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	// iterate over all repos

}

func saveRepo(ctx context.Context, store xdb.Store, repo *git.Repository) error {
	wt, _ := repo.Worktree()
	//status, _ := wt.Status()

	n := xdb.NewNode("Repo", wt.Filesystem.Root())

	attrs := []*xdb.Attr{
		n.Attr("path", xdb.String(wt.Filesystem.Root())),
		//	n.Attr("changes", xdb.Int(len(status))),
	}

	err := store.PutAttrs(ctx, attrs...)
	if err != nil {
		return err
	}

	got, err := store.GetAttrs(ctx, xdb.AttrRef(n, "path"))
	if err != nil {
		return err
	}

	slog.Info("Repo",
		"path", got[0].Value().String(),
		// "changes", got[1].Value().Int(),
	)

	return nil
}

func saveBranch(ctx context.Context, store xdb.Store, repo *git.Repository, branch *plumbing.Reference) error {
	n := xdb.NewNode("Branch", branch.Name().Short())

	attrs := []*xdb.Attr{
		n.Attr("name", xdb.String(branch.Name().Short())),
		n.Attr("hash", xdb.String(branch.Hash().String())),
	}

	err := store.PutAttrs(ctx, attrs...)
	if err != nil {
		return err
	}

	wt, _ := repo.Worktree()
	rn := xdb.NewNode("Repo", wt.Filesystem.Root())

	edges := []*xdb.Edge{
		rn.Edge("branches", n),
		n.Edge("repo", rn),
	}

	err = store.PutEdges(ctx, edges...)
	if err != nil {
		return err
	}

	refs := []xdb.Ref{
		xdb.AttrRef(n, "name"),
		xdb.AttrRef(n, "hash"),
	}

	got, err := store.GetAttrs(ctx, refs...)
	if err != nil {
		return err
	}

	slog.Info("Branch",
		"repo", rn.ID(),
		"name", got[0].Value().String(),
		"hash", got[1].Value().String(),
	)

	return nil
}

func saveCommit(ctx context.Context, store xdb.Store, repo *git.Repository, branch *plumbing.Reference, commit *object.Commit) error {
	wt, _ := repo.Worktree()

	rn := xdb.NewNode("Repo", wt.Filesystem.Root())
	bn := xdb.NewNode("Branch", branch.Name().Short())
	cn := xdb.NewNode("Commit", commit.Hash.String())
	un := xdb.NewNode("User", commit.Author.Email)

	edges := []*xdb.Edge{
		rn.Edge("commits", cn),
		bn.Edge("commits", cn),
		un.Edge("commits", cn),
		cn.Edge("repo", rn),
		cn.Edge("branch", bn),
		cn.Edge("user", un),
	}

	err := store.PutEdges(ctx, edges...)
	if err != nil {
		return err
	}

	attrs := []*xdb.Attr{
		cn.Attr("hash", xdb.String(commit.Hash.String())),
		cn.Attr("author", xdb.String(commit.Author.Email)),
		cn.Attr("message", xdb.String(commit.Message)),

		un.Attr("name", xdb.String(commit.Author.Name)),
		un.Attr("email", xdb.String(commit.Author.Email)),
	}

	err = store.PutAttrs(ctx, attrs...)
	if err != nil {
		return err
	}

	refs := []xdb.Ref{
		xdb.AttrRef(cn, "hash"),
		xdb.AttrRef(cn, "author"),
		xdb.AttrRef(cn, "message"),
	}

	got, err := store.GetAttrs(ctx, refs...)
	if err != nil {
		return err
	}

	slog.Info("Commit",
		"repo", rn.ID(),
		"branch", bn.ID(),
		"commit", cn.ID(),
		"user", un.ID(),
		"hash", got[0].Value().String(),
		"author", got[1].Value().String(),
		"message", got[2].Value().String(),
	)

	return nil
}
