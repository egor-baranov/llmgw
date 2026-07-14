package policy

import (
	"context"
	"math"
	"net/http"
	"strings"
	"sync"

	"llmgw/gateway"

	"github.com/samber/lo"
	"github.com/tiktoken-go/tokenizer"
)

// tokenizer.Get recompiles the encoding's regular expression. Keep one codec
// per encoding because token counting runs for every route candidate.
var (
	loadO200kTokenizer = sync.OnceValues(func() (tokenizer.Codec, error) {
		return tokenizer.Get(tokenizer.O200kBase)
	})
	loadCl100kTokenizer = sync.OnceValues(func() (tokenizer.Codec, error) {
		return tokenizer.Get(tokenizer.Cl100kBase)
	})
	loadP50kTokenizer = sync.OnceValues(func() (tokenizer.Codec, error) {
		return tokenizer.Get(tokenizer.P50kBase)
	})
	loadP50kEditTokenizer = sync.OnceValues(func() (tokenizer.Codec, error) {
		return tokenizer.Get(tokenizer.P50kEdit)
	})
	loadR50kTokenizer = sync.OnceValues(func() (tokenizer.Codec, error) {
		return tokenizer.Get(tokenizer.R50kBase)
	})
)

type TokenValidation struct {
	Counters   map[string]gateway.TokenCounter
	Builders   map[string]gateway.EffectiveParamBuilder
	Projectors map[string]gateway.TokenProjector
}

func (t TokenValidation) Wrap(next gateway.RequestHandler) gateway.RequestHandler {
	return func(ctx context.Context, state *gateway.RequestState) (*gateway.Execution, error) {
		candidates, err := state.ResolveCandidates()
		if err != nil {
			return nil, err
		}
		filtered := make([]gateway.ResolvedRoute, 0, len(candidates))
		var rejectedOutput bool
		var rejectedInput bool
		for _, candidate := range candidates {
			effective, estimate, err := t.materializeCandidate(ctx, candidate)
			if err != nil {
				continue
			}
			candidate.Effective = effective
			candidate.Estimate = estimate
			if maxInput := candidate.Route.Capabilities.MaxInputTokens; maxInput > 0 && estimate.InputTokens > int64(maxInput) {
				rejectedInput = true
				continue
			}
			requested := candidate.Request.RequestedMaxOutputTokens()
			if effective != nil && effective.MaxOutputTokens > 0 {
				requested = effective.MaxOutputTokens
			}
			if maxOutput := candidate.Route.Capabilities.MaxOutputTokens; maxOutput > 0 && requested > maxOutput {
				rejectedOutput = true
				continue
			}
			filtered = append(filtered, candidate)
		}
		if len(filtered) == 0 {
			switch {
			case rejectedInput:
				return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "max_input_tokens_exceeded", "estimated input tokens exceed configured maximum")
			case rejectedOutput:
				return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "max_output_tokens_exceeded", "requested max output tokens exceed configured maximum")
			default:
				return nil, gateway.UnsupportedOperation("no resolved route passed token validation")
			}
		}
		state.ReplaceCandidates(filtered)
		state.Estimate = maxEstimate(filtered)
		return next(ctx, state)
	}
}

func estimateUsage(req *gateway.Request, family string, projector gateway.TokenProjector) gateway.Usage {
	// Prefer the route's configured tokenizer over the decoder's cheap byte/rune
	// estimate. The latter remains a fallback for unknown tokenizer families.
	text := ""
	if len(req.RawBody) > 0 {
		if projector != nil {
			text = projector.ProjectTokenText(req.RawBody)
		}
		if text == "" {
			text = string(req.RawBody)
		}
	} else {
		text = req.PromptText()
	}
	in := countTokens(text, family)
	if in > 0 && (family == "claude" || family == "gemini") {
		// These families currently use cl100k as an approximation. Reserve a
		// safety margin until a provider-native tokenizer is installed.
		in = saturatingTokenMargin(in, 5, 4)
	}
	if in == 0 {
		// Unknown/missing tokenizer metadata must fail conservatively. A BPE
		// token cannot encode fewer than zero bytes, so raw UTF-8 byte length is
		// a safe upper bound and avoids CJK/emoji under-reservation.
		in = int64(len([]byte(text)))
	}
	if in == 0 {
		in = req.Hints.EstimatedInputTokens
	}
	if in == 0 && req.Meta.BodyBytes > 0 {
		in = req.Meta.BodyBytes / 4
		if in == 0 {
			in = 1
		}
	}
	if in == 0 && req.PromptText() != "" {
		in = int64(len([]rune(req.PromptText()))/4 + 1)
	}
	if in == 0 {
		in = 1
	}
	out := int64(req.RequestedMaxOutputTokens())
	if req.Operation == gateway.OpEmbeddings {
		out = 0
	}
	return gateway.Usage{
		InputTokens:  in,
		OutputTokens: out,
		TotalTokens:  saturatingAdd(in, out),
	}
}

func saturatingTokenMargin(value, numerator, denominator int64) int64 {
	if value <= 0 || numerator <= 0 || denominator <= 0 {
		return value
	}
	if value > (math.MaxInt64-(denominator-1))/numerator {
		return math.MaxInt64
	}
	return (value*numerator + denominator - 1) / denominator
}

func countTokens(text, family string) int64 {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	encoder, err := tokenizerForFamily(family)
	if err != nil {
		return 0
	}
	count, err := encoder.Count(text)
	if err != nil {
		return 0
	}
	return int64(count)
}

func tokenizerForFamily(family string) (tokenizer.Codec, error) {
	switch family {
	case "o200k_base":
		return loadO200kTokenizer()
	case "cl100k_base", "claude", "gemini":
		return loadCl100kTokenizer()
	case "p50k_base":
		return loadP50kTokenizer()
	case "p50k_edit":
		return loadP50kEditTokenizer()
	case "r50k_base":
		return loadR50kTokenizer()
	default:
		return nil, tokenizer.ErrEncodingNotSupported
	}
}

func (t TokenValidation) materializeCandidate(ctx context.Context, candidate gateway.ResolvedRoute) (*gateway.EffectiveParams, gateway.Usage, error) {
	family := candidate.Route.Capabilities.Tokenizer
	estimate := estimateUsage(candidate.Request, family, t.Projectors[candidate.Route.Provider])
	var effective *gateway.EffectiveParams
	if builder, ok := t.Builders[candidate.Route.Provider]; ok {
		built, err := builder.BuildEffective(candidate, candidate.Request)
		if err != nil {
			return nil, gateway.Usage{}, err
		}
		effective = built
		candidate.Effective = effective
	}
	if counter, ok := t.Counters[candidate.Route.Provider]; ok {
		if counted, err := counter.CountTokens(ctx, candidate, candidate.Request); err == nil {
			estimate = gateway.ReconcileReportedUsage(counted, estimate)
		}
	}
	estimate = reserveCandidateOutput(estimate, candidate, effective)
	estimate = addMultimodalSurcharges(estimate, candidate.Request.Hints, candidate.Route.Capabilities)
	if candidate.Request.Hints.MayWritePromptCache {
		estimate.CacheWriteTokens = estimate.InputTokens
	}
	estimate = addProviderUnitReservations(estimate, candidate.Request.Hints, candidate.Route.Pricing)
	return effective, estimate, nil
}

func addProviderUnitReservations(usage gateway.Usage, hints gateway.RequestHints, pricing gateway.Pricing) gateway.Usage {
	for _, unit := range hints.ProviderUnits {
		unitPricing, ok := pricing.ProviderUnits[unit]
		if !ok || unitPricing.MaxUnitsPerRequest <= 0 {
			continue
		}
		if usage.ProviderDetails == nil {
			usage.ProviderDetails = make(map[string]int64)
		}
		if usage.ProviderDetails[unit] < unitPricing.MaxUnitsPerRequest {
			usage.ProviderDetails[unit] = unitPricing.MaxUnitsPerRequest
		}
	}
	return usage
}

func reserveCandidateOutput(usage gateway.Usage, candidate gateway.ResolvedRoute, effective *gateway.EffectiveParams) gateway.Usage {
	if candidate.Request == nil || candidate.Request.Operation == gateway.OpEmbeddings {
		usage.OutputTokens = 0
		usage.TotalTokens = max(usage.TotalTokens, usage.InputTokens)
		return usage
	}
	requested := candidate.Request.RequestedMaxOutputTokens()
	if effective != nil && effective.MaxOutputTokens > 0 {
		requested = effective.MaxOutputTokens
	}
	if requested <= 0 && candidate.Route != nil {
		requested = candidate.Route.Capabilities.MaxOutputTokens
	}
	if requested <= 0 {
		// Routes without a declared maximum cannot provide a hard pre-call
		// bound. Retain a compatibility estimate, while validated production
		// routes should configure the provider/model maximum.
		requested = 256
	}
	multiplier := candidate.Request.Hints.OutputMultiplicity
	if multiplier <= 0 {
		multiplier = 1
	}
	reserved := saturatingMultiply(int64(requested), multiplier)
	if usage.OutputTokens < reserved {
		usage.OutputTokens = reserved
	}
	minimum := saturatingAdd(usage.InputTokens, usage.OutputTokens)
	if usage.TotalTokens < minimum {
		usage.TotalTokens = minimum
	}
	return usage
}

func addMultimodalSurcharges(usage gateway.Usage, hints gateway.RequestHints, capabilities gateway.Capability) gateway.Usage {
	vision := saturatingMultiply(hints.VisionInputParts, int64(capabilities.VisionInputTokenSurcharge))
	audio := saturatingMultiply(hints.AudioInputParts, int64(capabilities.AudioInputTokenSurcharge))
	surcharge := saturatingAdd(vision, audio)
	usage.InputTokens = saturatingAdd(usage.InputTokens, surcharge)
	usage.TotalTokens = saturatingAdd(usage.TotalTokens, surcharge)
	if minimum := saturatingAdd(usage.InputTokens, usage.OutputTokens); usage.TotalTokens < minimum {
		usage.TotalTokens = minimum
	}
	return usage
}

func saturatingMultiply(first, second int64) int64 {
	if first <= 0 || second <= 0 {
		return 0
	}
	if first > math.MaxInt64/second {
		return math.MaxInt64
	}
	return first * second
}

func saturatingAdd(first, second int64) int64 {
	if first < 0 {
		first = 0
	}
	if second < 0 {
		second = 0
	}
	if first > math.MaxInt64-second {
		return math.MaxInt64
	}
	return first + second
}

func maxEstimate(candidates []gateway.ResolvedRoute) gateway.Usage {
	out := lo.Reduce(candidates, func(acc gateway.Usage, candidate gateway.ResolvedRoute, _ int) gateway.Usage {
		if candidate.Estimate.InputTokens > acc.InputTokens {
			acc.InputTokens = candidate.Estimate.InputTokens
		}
		if candidate.Estimate.OutputTokens > acc.OutputTokens {
			acc.OutputTokens = candidate.Estimate.OutputTokens
		}
		if candidate.Estimate.TotalTokens > acc.TotalTokens {
			acc.TotalTokens = candidate.Estimate.TotalTokens
		}
		for unit, count := range candidate.Estimate.ProviderDetails {
			if acc.ProviderDetails == nil {
				acc.ProviderDetails = make(map[string]int64)
			}
			if count > acc.ProviderDetails[unit] {
				acc.ProviderDetails[unit] = count
			}
		}
		return acc
	}, gateway.Usage{})
	if out.TotalTokens == 0 {
		out.TotalTokens = saturatingAdd(out.InputTokens, out.OutputTokens)
	}
	return out
}
