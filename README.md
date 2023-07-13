# broadsky

Bridge [Streaming Wire Protocol (v0)](https://atproto.com/specs/event-stream#streaming-wire-protocol-v0) to [NATS](https://nats.io/) and other protocols

## Installation

```bash
go install github.com/atscan/broadsky/cmd/broadsky@latest
```

## Examples

```bash
# Bridge to NATS (using standard CBOR codec)
broadsky bridge nats bsky.social my-nats-server.com

# Bridge to NATS (using JSON codec)
broadsky bridge nats --codec json bsky.social my-nats-server.com
```

## Authors

- [tree 🌴](https://bsky.app/profile/did:plc:524tuhdhh3m7li5gycdn6boe)

## License

MIT