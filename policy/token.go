package policy

import (
	"context"
	"net/http"
	"strings"

	"llmgw/gateway"

	"github.com/samber/lo"
	"github.com/tiktoken-go/tokenizer"
)

type TokenValidation struct {
	Counters map[string]gateway.TokenCounter
	Builders map[string]gateway.EffectiveParamBuilder
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
			if maxInput := candidate.Route.Capabilities.MaxInputTokens; maxInput > 0 && int(estimate.InputTokens) > maxInput {
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

func estimateUsage(req *gateway.Request, family string) gateway.Usage {
	in := req.Hints.EstimatedInputTokens
	if in == 0 && req.Meta.BodyBytes > 0 {
		in = req.Meta.BodyBytes / 4
		if in == 0 {
			in = 1
		}
	}
	if in == 0 {
		in = countTokens(req.PromptText(), family)
	}
	if in == 0 && req.PromptText() != "" {
		in = int64(len([]rune(req.PromptText()))/4 + 1)
	}
	if in == 0 {
		in = 1
	}
	out := int64(req.RequestedMaxOutputTokens())
	if out == 0 && req.Operation != gateway.OpEmbeddings {
		out = 256
	}
	if req.Operation == gateway.OpEmbeddings {
		out = 0
	}
	return gateway.Usage{
		InputTokens:  in,
		OutputTokens: out,
		TotalTokens:  in + out,
	}
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
		return tokenizer.Get(tokenizer.O200kBase)
	case "cl100k_base", "claude", "gemini":
		return tokenizer.Get(tokenizer.Cl100kBase)
	case "p50k_base":
		return tokenizer.Get(tokenizer.P50kBase)
	case "p50k_edit":
		return tokenizer.Get(tokenizer.P50kEdit)
	case "r50k_base":
		return tokenizer.Get(tokenizer.R50kBase)
	default:
		return nil, tokenizer.ErrEncodingNotSupported
	}
}

func (t TokenValidation) materializeCandidate(ctx context.Context, candidate gateway.ResolvedRoute) (*gateway.EffectiveParams, gateway.Usage, error) {
	family := candidate.Route.Capabilities.Tokenizer
	estimate := estimateUsage(candidate.Request, family)
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
		if counted, err := counter.CountTokens(ctx, candidate, candidate.Request); err == nil && !counted.IsZero() {
			estimate = counted
		}
	}
	return effective, estimate, nil
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
		return acc
	}, gateway.Usage{})
	if out.TotalTokens == 0 {
		out.TotalTokens = out.InputTokens + out.OutputTokens
	}
	return out
}

func reserveEstimate(candidates []gateway.ResolvedRoute) gateway.EstimatedUsage {
	return lo.Reduce(candidates, func(acc gateway.EstimatedUsage, candidate gateway.ResolvedRoute, _ int) gateway.EstimatedUsage {
		if candidate.Estimate.InputTokens > acc.InputTokens {
			acc.InputTokens = candidate.Estimate.InputTokens
		}
		if candidate.Estimate.OutputTokens > acc.ReservedOutputTokens {
			acc.ReservedOutputTokens = candidate.Estimate.OutputTokens
		}
		if spend := estimatedSpendMicros(candidate.Route.Pricing, candidate.Estimate); spend > acc.EstimatedSpendMicros {
			acc.EstimatedSpendMicros = spend
		}
		return acc
	}, gateway.EstimatedUsage{})
}
