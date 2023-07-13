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
)

func main() {
	run(os.Args)
}

func run(args []string) {

	app := cli.App{
		Name:     "broadsky",
		Usage:    "bridge Streaming Wire Protocol (v0) to NATS and other protocols",
		Version:  "0.01",
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
			Usage: "Show messages in JSON (for debugging)",
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

				debug := cctx.Bool("debug")
				codec := cctx.String("codec")

				subject := cctx.Args().Get(2)
				if subject == "" {
					subject = "broadsky.stream.test"
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

				fmt.Fprintln(os.Stderr, "Bridge Started", time.Now().Format(time.RFC3339))
				defer func() {
					fmt.Fprintln(os.Stderr, "Bridge Exited", time.Now().Format(time.RFC3339))
				}()

				go func() {
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
