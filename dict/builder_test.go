package dict

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestZStdDict(t *testing.T) {
	for _, level := range []zstd.EncoderLevel{zstd.SpeedFastest, zstd.SpeedDefault, zstd.SpeedBetterCompression, zstd.SpeedBestCompression} {
		testZStdDict(t, level)
	}
}

func testZStdDict(t *testing.T, level zstd.EncoderLevel) {
	out := io.Discard
	if testing.Verbose() {
		out = os.Stdout
	}

	opts := Options{
		MaxDictSize:    2048,
		HashBytes:      4,
		Output:         out,
		ZstdDictID:     0,
		ZstdDictCompat: false,
		ZstdLevel:      level,
	}

	inBuf := make([]byte, 0, 4096)
	outBuf := make([]byte, 0, 4096)

	// This is 32K worth of data, but it's all very similar. Only fits in 4K if compressed with a dictionary.
	samples := generateSimilarByteSlices(42, 32)

	dict, err := BuildZstdDict(samples, opts)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(dict) > 2048 {
		t.Fatal("Dict Exceeds 2048 bytes")
	}

	totalSize := 0
	for _, blob := range samples {
		compressed, err := zCompressDict(inBuf, dict, blob)
		if err != nil {
			t.Fatal(err.Error())
		}
		totalSize += len(compressed)

		// Check round trip.
		decompressed, err := zDecompressDict(outBuf, dict, compressed)
		if err != nil {
			t.Fatal(err.Error())
		}
		if !bytes.Equal(decompressed, blob) {
			t.Fatal("Round trip failed")
		}
	}

	if totalSize > 4096 {
		t.Fatal("Total compressed size exceeds 4096 bytes")
	}
	t.Log("Total compressed size:", totalSize)
}

func zCompressDict(dst, dict, data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()

	result := encoder.EncodeAll(data, dst[:0])
	return result, nil
}

func zDecompressDict(dst, dict, data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	result, err := decoder.DecodeAll(data, dst)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Creates a slice of byte slices, each of which is has the same random seed, so they are very similar. The length
// of each slice is 1024 + the index of the slice.
func generateSimilarByteSlices(seed int64, count int) [][]byte {
	chks := make([][]byte, count)
	for i := 0; i < count; i++ {
		chks[i] = generateRandomByteSlice(seed, 1024+i)
	}

	return chks
}

func generateRandomByteSlice(seed int64, len int) []byte {
	r := rand.NewSource(seed)

	data := make([]byte, len)
	for i := range data {
		data[i] = byte(r.Int63())
	}
	return data
}
