package queue_reader

import (
	"testing"
)

type Tst struct {
	Input    string
	Expected ExportInfo
	IsError  bool
}

func TestGetExportInfo(t *testing.T) {
	tests := []Tst{
		Tst{
			Input: `<ns2:control99ProtocolMismatch schemeVersion="7.0">`,
			Expected: ExportInfo{
				Title:   "control99ProtocolMismatch",
				Version: "7.0",
			},
		},
	}

	for _, test := range tests {
		got, err := getExportInfo(test.Input)
		if err != nil && test.IsError {
			return
		}

		if *got != test.Expected {
			t.Fail()
		}
	}
}
