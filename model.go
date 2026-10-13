package turnstile

import (
	"math"
	"time"
)

// just a snapshot of different models, actual list would be larger.
type model string

const (
	// Google models
	g_gemini3_flash      model = "g_gemini3_flash"
	g_gemini3_1_flashlit model = "g_gemini3_1_flash_lite"
	g_gemini3_1_pro      model = "g_gemini3_1_pro"
	g_gemini3_5_flash    model = "g_gemini3_5_flash"
	g_gemini3_8_flash    model = "g_gemini3_8_flash"

	// Claude (Anthropic)
	c_haiku4_5  model = "c_haiku4_5"
	c_sonnet4_6 model = "c_sonnet4_6"
	c_sonnet5   model = "c_sonnet5"
	c_opus4_8   model = "c_opus4_8"
	c_opus5     model = "c_opus5"
	c_opus5_5   model = "c_opus5_5"

	// GPT (OpenAI)
	o_gpt4_1      model = "o_gpt4_1"
	o_gpt4_1_mini model = "o_gpt4_1_mini"
	o_gpt5_2      model = "o_gpt5_2"
	o_gpt5_4      model = "o_gpt5_4"
	o_gpt5_4_mini model = "o_gpt5_4_mini"
	o_gpt5_5      model = "o_gpt5_5"
	o_gpt5_6_sol  model = "o_gpt5_6_sol"

	// for the exercise taking a time limit.
	avgLatency = 2 * time.Second
	headroom   = 1.25 // 25% extra workers on top of Little's law
)

// just a snapshot of rate limits taken from https://www.palantir.com/docs/foundry/aip/llm-enrollment-rate-limits as an example.
var modelLimits = map[model]modelLimit{
	// Gemini — Vertex, Medium
	g_gemini3_flash:      {RPM: 1_400, TPM: 6_500_000},
	g_gemini3_1_flashlit: {RPM: 1_400, TPM: 6_500_000},
	g_gemini3_1_pro:      {RPM: 1_500, TPM: 7_000_000},
	g_gemini3_5_flash:    {RPM: 1_400, TPM: 6_500_000},
	g_gemini3_8_flash:    {RPM: 6_000, TPM: 12_000_000},

	// Claude — Anthropic, Medium
	c_haiku4_5:  {RPM: 750, TPM: 1_500_000},
	c_sonnet4_6: {RPM: 700, TPM: 1_500_000},
	c_sonnet5:   {RPM: 700, TPM: 1_500_000},
	c_opus4_8:   {RPM: 200, TPM: 1_000_000},
	c_opus5:     {RPM: 200, TPM: 1_000_000},
	c_opus5_5:   {RPM: 200, TPM: 1_000_000},

	// GPT — OpenAI, Medium
	o_gpt4_1:      {RPM: 1_500, TPM: 2_000_000},
	o_gpt4_1_mini: {RPM: 2_000, TPM: 3_000_000},
	o_gpt5_2:      {RPM: 1_500, TPM: 3_000_000},
	o_gpt5_4:      {RPM: 1_500, TPM: 3_000_000},
	o_gpt5_4_mini: {RPM: 1_500, TPM: 3_000_000},
	o_gpt5_5:      {RPM: 1_500, TPM: 3_000_000},
	o_gpt5_6_sol:  {RPM: 5_000, TPM: 10_000_000},
}

type modelLimit struct {
	RPM float64 // provider's requests per minute
	TPM float64 // provider's tokens per minute
}

// workers ≈ (requests per second the provider allows) × (average LLM call duration)
// apply littles law here  to get the proper count. taking avg llm say 5s
// if RPM of geminin is 1000RPM then req/sec is 16.7 . so 16.7 *5 = ~80
// add lets say 25% wait room 80 +20 = 100.
func getWorkerConfig() map[model]int {
	out := make(map[model]int, len(modelLimits))
	for m, l := range modelLimits {
		perSecond := l.RPM / 60
		out[m] = int(math.Ceil(perSecond * avgLatency.Seconds() * headroom))
	}
	return out
}
