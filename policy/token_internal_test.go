package policy

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"llmgw/gateway"

	"github.com/tiktoken-go/tokenizer"
)

func TestTokenizerForFamilyCachesConcurrentCounts(t *testing.T) {
	const (
		workers    = 16
		iterations = 4
		text       = "Caching a compiled tokenizer must preserve deterministic counts under concurrent gateway traffic."
	)
	tests := []struct {
		name     string
		family   string
		encoding tokenizer.Encoding
	}{
		{name: "o200k", family: "o200k_base", encoding: tokenizer.O200kBase},
		{name: "cl100k", family: "cl100k_base", encoding: tokenizer.Cl100kBase},
		{name: "p50k", family: "p50k_base", encoding: tokenizer.P50kBase},
		{name: "p50k edit", family: "p50k_edit", encoding: tokenizer.P50kEdit},
		{name: "r50k", family: "r50k_base", encoding: tokenizer.R50kBase},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reference, err := tokenizer.Get(test.encoding)
			if err != nil {
				t.Fatal(err)
			}
			want, err := reference.Count(text)
			if err != nil {
				t.Fatal(err)
			}

			start := make(chan struct{})
			codecs := make(chan tokenizer.Codec, workers)
			errs := make(chan error, workers)
			var wg sync.WaitGroup
			wg.Add(workers)
			for worker := range workers {
				go func() {
					defer wg.Done()
					<-start
					codec, err := tokenizerForFamily(test.family)
					if err != nil {
						errs <- fmt.Errorf("worker %d loading tokenizer: %w", worker, err)
						return
					}
					for range iterations {
						got, err := codec.Count(text)
						if err != nil {
							errs <- fmt.Errorf("worker %d counting tokens: %w", worker, err)
							return
						}
						if got != want {
							errs <- fmt.Errorf("worker %d count = %d, want %d", worker, got, want)
							return
						}
					}
					codecs <- codec
				}()
			}
			close(start)
			wg.Wait()
			close(codecs)
			close(errs)

			for err := range errs {
				t.Error(err)
			}
			var first tokenizer.Codec
			count := 0
			for codec := range codecs {
				if first == nil {
					first = codec
				} else if codec != first {
					t.Error("concurrent calls returned different cached codecs")
				}
				count++
			}
			if count != workers {
				t.Fatalf("successful workers = %d, want %d", count, workers)
			}
		})
	}

	cl100k, err := tokenizerForFamily("cl100k_base")
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"claude", "gemini"} {
		codec, err := tokenizerForFamily(alias)
		if err != nil {
			t.Fatalf("%s tokenizer: %v", alias, err)
		}
		if codec != cl100k {
			t.Errorf("%s did not reuse the cl100k codec", alias)
		}
	}
}

func TestMaxEstimateSaturatesMissingTotal(t *testing.T) {
	got := maxEstimate([]gateway.ResolvedRoute{{Estimate: gateway.Usage{InputTokens: math.MaxInt64, OutputTokens: 1}}})
	if got.TotalTokens != math.MaxInt64 {
		t.Fatalf("total tokens = %d, want saturated %d", got.TotalTokens, int64(math.MaxInt64))
	}
}

func BenchmarkCountTokensCached(b *testing.B) {
	const text = "Caching a compiled tokenizer avoids rebuilding its regular expression and codec on every request."
	if got := countTokens(text, "cl100k_base"); got == 0 {
		b.Fatal("token count is zero")
	}
	b.ReportAllocs()

	var got int64
	for b.Loop() {
		got = countTokens(text, "cl100k_base")
	}
	if got == 0 {
		b.Fatal("token count is zero")
	}
}
