# Debugging Notes

Hard-won technical findings from building the streaming pipeline. Kept as a reference for
anyone touching the MP4 parser, fMP4 fragmentation, or browser MSE playback. (Consolidated
from earlier development-journal and `test_utils/` scratch files.)

## Chrome AAC / ESDS codec compatibility

The recurring failure was Chrome rejecting audio with:

```
CHUNK_DEMUXER_ERROR_APPEND_FAILED: audio object type 0x... does not match
what is specified in the mimetype
```

Key facts that resolved most of it:

- Chrome validates **both** the codec string *and* the binary `esds` contents, and they must
  agree.
- In the ESDS descriptor, keep **`objectTypeIndication = 0x40`** (MPEG-4 Audio) while ensuring
  the **`AudioSpecificConfig.audioObjectType = 2`** (AAC-LC). The two live in different places
  and are easy to conflate.
- The correct AudioSpecificConfig byte for AAC-LC @ 44.1 kHz stereo is `0x11 0x90`
  (`audioObjectType=2`, sampling-frequency-index, channel-config).
- Advertise the matching codec string: `audio/mp4; codecs="mp4a.40.2"` (RFC 6381).

Relevant code: `core/src/media/mp4_parser.rs`, `core/src/media/fmp4_converter.rs`,
`core/src/media/fragment_writer.rs`.

### Why you can't "fix" it by rewriting the object type

Chrome's MSE imposes three constraints at once, and they conflict:

1. **Codec-string whitelist** — only predefined strings are accepted: `mp4a.40.2` (AAC-LC),
   `mp4a.40.5` (HE-AAC), `mp4a.40.29` (HE-AAC v2). `mp4a.02.2` is *not* recognized.
2. **Binary/string agreement** — the `esds` object type must match what the codec string implies.
3. **Compatibility filtering** — Chrome is stricter than the spec in places.

Attempts that *don't* work:
- Rewrite `esds` object type `0x40 → 0x02` but keep `mp4a.40.2` → "object type 0x2 does not match".
- Also switch codec string to `mp4a.02.2` → "codec not supported" (not in whitelist).

What works is leaving `objectTypeIndication = 0x40` and advertising `mp4a.40.2`, with a correct
`AudioSpecificConfig` (`audioObjectType = 2`). Firefox/Safari/FFmpeg accept the same stream
without the whitelist gymnastics, so always validate Chrome separately.

## MSE-compatible fMP4 fragmentation

For MediaSource to accept the stream the initialization segment must be a proper
`ftyp + moov` where the `moov` contains an `mvex` (Movie Extends) box with a `trex`
(Track Extends) box per track (video / audio). Media segments then follow as
`moof + mdat` fragments with accurate timestamps and durations.

- Missing `mvex`/`trex` is a common cause of silent MSE append failures.
- Keyframe detection drives fragment boundaries — fragments should start on a keyframe.

## Cross-browser notes

A standards-correct stream is accepted by Firefox and Safari MediaSource, FFmpeg, and
ISO-BMFF validators. Chrome's MediaSource is the strictest consumer and historically had
the most undocumented edge cases, so test Chrome explicitly when changing container output.

## Regenerating a test video

Use `scripts/make_test_video.sh` to synthesize `test_simple.mp4` (H.264 + AAC) with no
source file. For converting an existing file to MSE-friendly fragmented MP4, see
`scripts/prepare_mse_video.sh`.
