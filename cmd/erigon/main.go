// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"cmp"
	"fmt"
	"net/http"
	"os"
	"runtime/debug"

	"github.com/anacrolix/envpprof"
	"github.com/felixge/fgprof"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/diagnostics"
	erigonapp "github.com/erigontech/erigon/turbo/app"
	erigoncli "github.com/erigontech/erigon/turbo/cli"
	turbodebug "github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/node"
)

// configureGC sets GC tuning parameters early in startup to reduce GC overhead on large heaps.
// GOGC=50 triggers GC more frequently at smaller heap growth, reducing peak memory and pause times.
// GOMEMLIMIT is left to the operator via the GOMEMLIMIT environment variable.
func configureGC() {
	prev := debug.SetGCPercent(50)
	fmt.Fprintf(os.Stderr, "[gc-tuning] GOGC set to 50 (was %d). Set GOMEMLIMIT env var to control memory limit.\n", prev)
}

func main() {
	configureGC()
	defer envpprof.Stop()
	http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
	app := erigonapp.MakeApp("erigon", runErigon, erigoncli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		envpprof.Stop()
		os.Exit(1)
	}
}

func runErigon(cliCtx *cli.Context) (err error) {
	logger, tracer, metricsMux, pprofMux, err := turbodebug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return
	}

	debugMux := cmp.Or(metricsMux, pprofMux)

	// Log the full command used to start the program (with sensitive info like URLs and IP addresses redacted)
	logger.Info("Startup command", "cmd", log.RedactArgs(os.Args))

	// initializing the node and providing the current git commit there

	logger.Info("Build info", "git_branch", version.GitBranch, "git_tag", version.GitTag, "git_commit", version.GitCommit)
	if version.Major == 3 {
		logger.Info(`
	########b          oo                               d####b. 
	##                                                      '## 
	##aaaa    ##d###b. dP .d####b. .d####b. ##d###b.     aaad#' 
	##        ##'  '## ## ##'  '## ##'  '## ##'  '##        '## 
	##        ##       ## ##.  .## ##.  .## ##    ##        .## 
	########P dP       dP '####P## '#####P' dP    dP    d#####P 
	                           .##                              
	                       d####P                               
		`)
	}
	erigonInfoGauge := metrics.GetOrCreateGauge(fmt.Sprintf(`erigon_info{version="%s",commit="%s"}`, version.VersionNoMeta, version.GitCommit))
	erigonInfoGauge.Set(1)

	nodeCfg, err := node.NewNodConfigUrfave(cliCtx, debugMux, logger)
	if err != nil {
		return err
	}
	if err := datadir.ApplyMigrations(nodeCfg.Dirs); err != nil {
		return err
	}

	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg, logger)

	ethNode, err := node.New(cliCtx.Context, nodeCfg, ethCfg, logger, tracer)
	if err != nil {
		log.Error("Erigon startup", "err", err)
		return err
	}

	diagnostics.Setup(cliCtx, ethNode, metricsMux, pprofMux)

	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving an Erigon node", "err", err)
	}
	return err
}
