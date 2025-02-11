package translator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrimSingleQuotes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "With single quotes",
			input:    "'Hello, World!'",
			expected: "Hello, World!",
		},
		{
			name:     "Without single quotes",
			input:    "Hello, World!",
			expected: "Hello, World!",
		},
		{
			name:     "Only quotes",
			input:    "''",
			expected: "",
		},
		{
			name:     "Spaces with quotes",
			input:    "'   '",
			expected: "   ", // maintaining spaces
		},
		{
			name:     "Single quote at start only",
			input:    "'Hello, World!",
			expected: "'Hello, World!", // single quote not at the end should remain
		},
		{
			name:     "Single quote at end only",
			input:    "Hello, World!'",
			expected: "Hello, World!'", // single quote not at the start should remain
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := trimSingleQuotes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
