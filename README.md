# Chatter node

The Go backend of [Chatter](https://github.com/rishasabi/p2p_chatter), an
offline mesh messenger. Every phone runs one node: it finds the phones
around it, signs and relays messages hop by hop, carries mail for people
who are out of range, and serves a small HTTP API to the app on the same
phone. There is no server anywhere.

On Android the node runs inside the app, built as `libchatter.so`. For
development it also runs as a standalone binary (`cmd/node`).

## What it does

- **Identity.** Each node has an Ed25519 key, kept in `<data-dir>/identity.key`.
  Its libp2p peer ID is the node's address on the mesh.
- **Wi-Fi links.** libp2p over TCP (port 4001, or any free port if that's
  taken), encrypted with Noise and multiplexed with Yamux. Peers on the same
  network or hotspot are found with mDNS, by scanning a QR code
  (`POST /peers/connect`), or through peer exchange: neighbours pass on who
  else they can reach.
- **Bluetooth links.** Go can't drive the phone's radio, so the app does
  (BLE L2CAP, in Kotlin). The node hands it frames to send and takes the
  frames it receives through `/bt/outbox` and `/bt/inbox`, and treats those
  links like any other.
- **Relaying.** Messages travel as signed envelopes with a hop limit (TTL 8,
  at most 16). A node delivers what's addressed to it and passes the rest on.
  A seen cache drops repeats, so floods don't loop.
- **Store, carry, forward.** A message that can't reach its recipient yet is
  kept for 24 hours (up to 500) and offered to every neighbour that turns up.
  Channel posts are kept for 6 hours (up to 200).
- **Channels.** Open channels go by name. Private ones combine a name with a
  passphrase: payloads are sealed with AES-256-GCM under a PBKDF2 key, and
  relays see only a tag, never the name or the text. A phone that joins a
  channel asks its neighbours for the last 12 hours of posts and checks every
  signature before accepting them.
- **Receipts.** Delivery receipts go back automatically, and read receipts
  are sent when the app asks for them.

## How a message travels

1. The app posts it to `POST /message`, for a peer or a channel.
2. The node wraps it in an envelope with a random ID, the sender's peer ID,
   a timestamp and a TTL, and signs every field except the TTL, so relays
   can count hops down without breaking the signature.
3. It goes straight to the recipient if they're a neighbour. Otherwise it's
   flooded to every neighbour, over Wi-Fi and Bluetooth alike.
4. Each relay checks the signature, drops repeats, lowers the TTL and passes
   it on. If nobody is in range, the message is carried until someone is.
5. The recipient keeps it for the app (`GET /messages`) and sends a delivery
   receipt back the same way.

## Layout

| Path | What's there |
| --- | --- |
| `node/` | Starts the libp2p host: identity, mDNS, liveness pings, handles, peer exchange |
| `processing/` | Envelopes, signatures, relaying, carrying, channels and their encryption, history catch-up, receipts |
| `transport/` | The `Network` interface, libp2p streams, and the Bluetooth bridge |
| `api/http/` | The local HTTP API |
| `embedded/` | C exports for the app: `StartNode`, `StopNode`, `IsNodeRunning` |
| `cmd/node/` | The standalone binary |
| `tests/` | Integration tests that run several nodes against each other |

## Local API

The API listens on `127.0.0.1` only (port 8080 by default), so only apps on
the same phone can reach it. All bodies are JSON.

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Is the node up |
| `GET /node` | Peer ID, addresses and handle |
| `GET`, `POST /profile` | Read or set the handle |
| `GET /peers` | Connected Wi-Fi peers |
| `POST /peers/connect` | Dial a peer's address, e.g. from a QR code |
| `GET /peers/discovered` | Peers that neighbours told us about |
| `POST /message` | Send to a peer (`peer_id`) or a channel (`room`) |
| `GET /messages?after=N` | Messages received since N |
| `GET`, `POST /receipts` | Receipts received since N, or send one |
| `POST`, `DELETE /rooms` | Join or leave a channel |
| `POST /bt/peers` | The app's current Bluetooth neighbours |
| `GET /bt/outbox?wait=S` | Long-poll for frames to send over Bluetooth |
| `POST /bt/inbox` | A frame the app received over Bluetooth |

## Run and test

Go 1.26.5 or newer:

```sh
go run ./cmd/node -port 8080 -p2p-port 4001 -data-dir ./data
go test ./...
```

Two nodes on one machine need different ports and data directories.

## Build for Android

The app loads the node as `libchatter.so`, one per ABI, from
`app/android/app/src/main/jniLibs/<abi>/`. Cross-compile it with the
Android NDK's clang, for API level 24 (the app's minimum):

```sh
NDK=$ANDROID_HOME/ndk/27.0.12077973/toolchains/llvm/prebuilt/linux-x86_64/bin
CGO_ENABLED=1 GOOS=android GOARCH=arm64 CC=$NDK/aarch64-linux-android24-clang \
  go build -buildmode=c-shared -trimpath -ldflags="-s -w -checklinkname=0" \
  -o ../app/android/app/src/main/jniLibs/arm64-v8a/libchatter.so ./embedded
```

| ABI | `GOARCH` | `CC` |
| --- | --- | --- |
| `arm64-v8a` | `arm64` | `aarch64-linux-android24-clang` |
| `armeabi-v7a` | `arm`, with `GOARM=7` | `armv7a-linux-androideabi24-clang` |
| `x86_64` | `amd64` | `x86_64-linux-android24-clang` |

On Windows the toolchain is under `prebuilt/windows-x86_64`, and the
compilers end in `.cmd`. `-s -w` and `-trimpath` keep the library small.
`-checklinkname=0` is needed by `wlynxg/anet`, which libp2p uses to list
network interfaces on Android. The release workflow in the app repository
builds all three from the commit this submodule is pinned to.

## Limits

- Direct messages are signed but not end-to-end encrypted, so phones that
  relay them can read them. Only private channels are encrypted end to end.
- The libp2p host also accepts plaintext links when a peer doesn't offer
  Noise.
- Carried messages and the message store live in memory, so a relay's copies
  are lost when its app stops. Delivery only moves while the app is running.
