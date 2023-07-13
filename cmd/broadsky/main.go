package main

import (
	//"bufio"
	//"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	//"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	cbor "github.com/fxamacker/cbor/v2"

	cli "github.com/urfave/cli/v2"

	version "github.com/atscan/broadsky/util"
)

type metricsData struct {
	total   int
	counts  map[string]int
	mcounts map[string]int
}

func main() {
	run(os.Args)
}

func run(args []string) {

	app := cli.App{
		Name:     "broadsky",
		Usage:    "bridge Streaming Wire Protocol (v0) to NATS and other protocols",
		Version:  version.Version,
		Compiled: time.Now(),
		Authors: []*cli.Author{
			&cli.Author{
				Name:  "tree 🌴",
				Email: "tree@tree.fail",
			},
		},
	}

	app.Commands = []*cli.Command{
		bridgeCmd,
	}

	app.RunAndExitOnError()
}

var bridgeCmd = &cli.Command{
	Name:      "bridge",
	Aliases:   []string{"b"},
	Usage:     "Run a bridge",
	ArgsUsage: `<repo> [<protocol-options>]`,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "cursor",
			Usage:    "Cursor for source repo",
			Category: "Input options:",
		},
		&cli.BoolFlag{
			Name:  "debug",
			Value: false,
			Usage: "Show messages in JSON (for debugging)",
		},
		&cli.BoolFlag{
			Name:  "metrics",
			Value: false,
			Usage: "Metrics HTTP endpoint /_metrics",
		},
		&cli.StringFlag{
			Name:  "metrics-listen",
			Value: "127.0.0.1:5212",
			Usage: "Metrics host and port",
		},
	},
	Subcommands: []*cli.Command{
		{
			Name:      "nats",
			Usage:     "Bridge to NATS",
			ArgsUsage: `<repo> [target] [<subject>]`,
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "codec",
					Value: "cbor",
					Usage: "Specify output codec: cbor, json",
				},
			},
			Action: func(cctx *cli.Context) error {
				ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT)
				defer stop()

				repo := cctx.Args().First()
				if repo == "" {
					return fmt.Errorf("Please provide repo source, for example: wss://bsky.social")
				}
				if !strings.Contains(repo, "subscribeRepos") {
					repo = repo + "/xrpc/com.atproto.sync.subscribeRepos"
				}
				re := regexp.MustCompile(`^wss?:\/\/`)
				if !re.Match([]byte(repo)) {
					repo = "wss://" + repo
				}

				if cctx.String("cursor") != "" {
					repo = fmt.Sprintf("%s?cursor=%s", repo, cctx.String("cursor"))
				}

				target := cctx.Args().Get(1)
				if target == "" {
					target = nats.DefaultURL
				}

				subject := cctx.Args().Get(2)
				if subject == "" {
					subject = "broadsky.stream.test"
				}

				debug := cctx.Bool("debug")
				codec := cctx.String("codec")
				metrics := cctx.Bool("metrics")

				// initialize metrics http endpoint
				stats := metricsData{0, make(map[string]int), make(map[string]int)}

				var metricsQuit chan os.Signal
				if metrics {
					metricsListen := cctx.String("metrics-listen")

					http.HandleFunc("/_metrics", func(w http.ResponseWriter, r *http.Request) {
						if debug {
							fmt.Printf("HTTP %s %s%s\n", r.Method, r.Host, r.URL)
						}

						if r.URL.Path != "/_metrics" {
							http.Error(w, "Not Found", http.StatusNotFound)
							return
						}

						w.Header().Set("Content-Type", "text/plain")

						m := fmt.Sprintf("broadsky_bridge_events_total{server=\"nil\",repo=\"%v\"} %v\n", repo, stats.total)
						for key, n := range stats.counts {
							re := regexp.MustCompile("^([^:]+):app.bsky.([^\\/]+)")
							match := re.FindStringSubmatch(key)
							if match == nil {
								continue
							}
							m += fmt.Sprintf("broadsky_bridge_events{server=\"nil\",repo=\"%v\",action=\"%v\",type=\"%v\"} %v\n", repo, match[1], match[2], n)
						}

						w.Write([]byte(m))
					})

					fmt.Fprintf(os.Stderr, "Metrics endpoint active: http://%v\n", metricsListen)

					httpServer := http.Server{
						Addr: metricsListen,
					}

					go func() {
						if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
							fmt.Errorf("HTTP server ListenAndServe Error: %v", err)
						}
					}()

					metricsQuit = make(chan os.Signal, 1)
					signal.Notify(metricsQuit, os.Interrupt)
				}

				// open target nats connection
				fmt.Fprintln(os.Stderr, "dialing NATS target:", target)
				nc, err := nats.Connect(target)
				if err != nil {
					return fmt.Errorf("NATS dial failure: %w", err)
				}
				defer nc.Drain()
				fmt.Fprintln(os.Stderr, "NATS connected, using base subject:", subject)
				jc, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
				defer jc.Close()

				// open source websocket connection
				fmt.Fprintln(os.Stderr, "dialing websocket source:", repo)
				d := websocket.DefaultDialer
				con, _, err := d.Dial(repo, http.Header{})
				if err != nil {
					return fmt.Errorf("ws dial failure: %w", err)
				}

				banner()
				fmt.Fprintln(os.Stderr, "Bridge Started", time.Now().Format(time.RFC3339))
				defer func() {
					fmt.Fprintln(os.Stderr, "Bridge Exited", time.Now().Format(time.RFC3339))
				}()

				go func() {
					<-metricsQuit
					<-ctx.Done()
					_ = con.Close()
				}()

				rsc := &events.RepoStreamCallbacks{
					RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {

						b, err := encode(codec, evt)
						if err != nil {
							return err
						}
						nc.Publish(subject+".commit", b)

						for _, op := range evt.Ops {
							opkey := op.Action + ":" + strings.Split(op.Path, "/")[0]
							stats.total++
							if _, ok := stats.counts[opkey]; !ok {
								stats.counts[opkey] = 0
							}
							stats.counts[opkey]++
						}

						if debug {
							b, err := json.Marshal(evt)
							if err != nil {
								return err
							}
							var out map[string]any
							if err := json.Unmarshal(b, &out); err != nil {
								return err
							}
							out["blocks"] = fmt.Sprintf("[%d bytes]", len(evt.Blocks))
							b, err = json.Marshal(out)
							if err != nil {
								return err
							}
							fmt.Println(string(b))
						}
						return nil
					},
					RepoHandle: func(handle *comatproto.SyncSubscribeRepos_Handle) error {

						b, err := json.Marshal(handle)
						if err != nil {
							return err
						}
						if debug {
							fmt.Println(string(b))
						}
						return nil

					},
					RepoInfo: func(info *comatproto.SyncSubscribeRepos_Info) error {

						b, err := json.Marshal(info)
						if err != nil {
							return err
						}
						fmt.Println(string(b))
						return nil
					},
				}
				return events.HandleRepoStream(ctx, con, &events.SequentialScheduler{rsc.EventHandler})
			},
		},
	},
}

func encode(codec string, evt interface{}) ([]byte, error) {
	var b []byte
	var err error
	if codec == "json" {
		b, err = json.Marshal(evt)
		if err != nil {
			return nil, err
		}
	} else {
		b, err = cbor.Marshal(evt)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func banner() {
	fmt.Fprintln(os.Stderr, "\n"+
		" _ )  _ \\   _ \\     \\    _ \\    __|  |  / \\ \\  / \n"+
		" _ \\    /  (   |   _ \\   |  | \\__ \\  . <   \\  / \n"+
		"___/ _|_\\ \\___/  _/  _\\ ___/  ____/ _|\\_\\   _|  \n")
}
