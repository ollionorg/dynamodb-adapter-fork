// Copyright 2021
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"reflect"
	"testing"

	"cloud.google.com/go/spanner"
	translator "github.com/cloudspannerecosystem/dynamodb-adapter/translator/utils"
)

func Test_parseRow(t *testing.T) {
	tests := []struct {
		name      string
		row       *spanner.Row
		colDDL    map[string]string
		want      map[string]interface{}
		wantError bool
	}{
		{
			name: "ParseStringValue",
			row: func() *spanner.Row {
				row, err := spanner.NewRow([]string{"strCol"}, []interface{}{
					spanner.NullString{StringVal: "my-text", Valid: true},
				})
				if err != nil {
					t.Fatalf("failed to create row: %v", err)
				}
				return row
			}(),
			colDDL: map[string]string{"strCol": "S"},
			want:   map[string]interface{}{"strCol": "my-text"},
		},
		{
			name: "ParseIntValue",
			row: func() *spanner.Row {
				row, err := spanner.NewRow([]string{"intCol"}, []interface{}{
					spanner.NullFloat64{Float64: 314, Valid: true},
				})
				if err != nil {
					t.Fatalf("failed to create row: %v", err)
				}
				return row
			}(),
			colDDL: map[string]string{"intCol": "N"},
			want:   map[string]interface{}{"intCol": 314.0},
		},
		{
			name: "ParseFloatValue",
			row: func() *spanner.Row {
				row, err := spanner.NewRow([]string{"floatCol"}, []interface{}{
					spanner.NullFloat64{Float64: 3.14, Valid: true},
				})
				if err != nil {
					t.Fatalf("failed to create row: %v", err)
				}
				return row
			}(),
			colDDL: map[string]string{"floatCol": "N"},
			want:   map[string]interface{}{"floatCol": 3.14},
		},
		{
			name: "ParseBoolValue",
			row: func() *spanner.Row {
				row, err := spanner.NewRow([]string{"boolCol"}, []interface{}{
					spanner.NullBool{Bool: true, Valid: true},
				})
				if err != nil {
					t.Fatalf("failed to create row: %v", err)
				}
				return row
			}(),
			colDDL: map[string]string{"boolCol": "BOOL"},
			want:   map[string]interface{}{"boolCol": true},
		},
		{
			name: "RemoveNulls",
			row: func() *spanner.Row {
				row, err := spanner.NewRow([]string{"strCol"}, []interface{}{
					spanner.NullString{StringVal: "", Valid: false},
				})
				if err != nil {
					t.Fatalf("failed to create row: %v", err)
				}
				return row
			}(),
			colDDL: map[string]string{"strCol": "S"},
			want:   map[string]interface{}{}, // Null value should be removed
		},
		{
			name: "SkipCommitTimestamp",
			row: func() *spanner.Row {
				row, err := spanner.NewRow([]string{"commit_timestamp"}, []interface{}{
					nil, // Commit timestamp should be skipped
				})
				if err != nil {
					t.Fatalf("failed to create row: %v", err)
				}
				return row
			}(),
			colDDL: map[string]string{"commit_timestamp": "S"},
			want:   map[string]interface{}{}, // Commit timestamp should not appear in the result
		},
		{
			name: "MultiValueRow",
			row: func() *spanner.Row {
				row, err := spanner.NewRow([]string{"boolCol", "intCol", "strCol"}, []interface{}{
					spanner.NullBool{Bool: true, Valid: true},
					spanner.NullFloat64{Float64: 32, Valid: true},
					spanner.NullString{StringVal: "my-text", Valid: true},
				})
				if err != nil {
					t.Fatalf("failed to create row: %v", err)
				}
				return row
			}(),
			colDDL: map[string]string{"boolCol": "BOOL", "intCol": "N", "strCol": "S"},
			want:   map[string]interface{}{"boolCol": true, "intCol": 32.0, "strCol": "my-text"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRow(tt.row, tt.colDDL)
			if (err != nil) != tt.wantError {
				t.Errorf("parseRow() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildStmt(t *testing.T) {
	// Set up the test data
	query := &translator.DeleteUpdateQueryMap{
		SpannerQuery: "UPDATE Users SET age = @age WHERE name = @name",
		Params: map[string]interface{}{
			"age":  30,
			"name": "John Doe",
		},
	}

	// Expected result
	expectedStmt := &spanner.Statement{
		SQL:    query.SpannerQuery,
		Params: query.Params,
	}

	// Call the function
	result := buildStmt(query)

	// Assert that the result matches the expected statement
	if result.SQL != expectedStmt.SQL {
		t.Errorf("Expected SQL: %s, but got: %s", expectedStmt.SQL, result.SQL)
	}

	for key, expectedValue := range expectedStmt.Params {
		if result.Params[key] != expectedValue {
			t.Errorf("Expected param[%s]: %v, but got: %v", key, expectedValue, result.Params[key])
		}
	}
}

func TestBuildCommitOptions(t *testing.T) {
	storage := Storage{}

	// Call the function to test
	options := storage.BuildCommitOptions()

	// Verify that MaxCommitDelay is not nil
	if options.MaxCommitDelay == nil {
		t.Fatal("Expected MaxCommitDelay to be non-nil")
	}

	// Verify that it matches the expected default commit delay
	expectedDelay := defaultCommitDelay
	if *options.MaxCommitDelay != expectedDelay {
		t.Errorf("Expected MaxCommitDelay: %v, but got: %v", expectedDelay, *options.MaxCommitDelay)
	}
}
