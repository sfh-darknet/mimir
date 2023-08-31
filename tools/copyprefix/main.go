// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/util/objtools"
)

type config struct {
	copyConfig objtools.CopyBucketConfig
	prefix     string
	overwrite  bool
	dryRun     bool
}

func (c *config) RegisterFlags(f *flag.FlagSet) {
	c.copyConfig.RegisterFlags(f)
	f.StringVar(&c.prefix, "prefix", "", "The prefix to copy. If the prefix is not empty and does not end in '"+objtools.Delim+"' then it is appended.")
	f.BoolVar(&c.overwrite, "overwrite", true, "If true existing objects in the destination bucket will be overwritten, otherwise they will be skipped.")
	f.BoolVar(&c.dryRun, "dry-run", false, "If true no copying will actually occur and instead a log message will be written.")
}

func (c *config) Validate() error {
	if err := c.copyConfig.Validate(); err != nil {
		return err
	}

	return nil
}

func main() {
	cfg := config{}
	cfg.RegisterFlags(flag.CommandLine)

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	logger := log.NewLogfmtLogger(os.Stdout)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := runCopy(ctx, cfg, logger); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func runCopy(ctx context.Context, cfg config, logger log.Logger) error {
	sourceBucket, destBucket, copyFunc, err := cfg.copyConfig.ToBuckets(ctx)
	if err != nil {
		return err
	}

	prefix := cfg.prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	sourceNames, err := listNames(ctx, sourceBucket, prefix)
	if err != nil {
		return err
	}

	var exists map[string]struct{}
	if cfg.overwrite {
		destNames, err := listNames(ctx, destBucket, prefix)
		if err != nil {
			return err
		}
		exists := make(map[string]struct{}, len(destNames))
		for _, name := range destNames {
			exists[name] = struct{}{}
		}
	}

	for _, name := range sourceNames {
		if _, ok := exists[name]; ok {
			logger.Log("Skipping copying {} since it exists in the destination bucket.", name)
			continue
		}
		if cfg.dryRun {
			logger.Log("Would have copied {}, but skipping due to dry run.", name)
			continue
		}
		err := copyFunc(ctx, name)
		if err != nil {
			return err
		}
	}

	return nil
}

func listNames(ctx context.Context, bucket objtools.Bucket, prefix string) ([]string, error) {
	listing, err := bucket.ListPrefix(ctx, prefix, true)
	if err != nil {
		return nil, err
	}
	if prefix != "" {
		for i, name := range listing {
			listing[i] = prefix + name
		}
	}
	return listing, nil
}
