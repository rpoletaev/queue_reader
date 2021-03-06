package queue_reader

import (
	"testing"
)

func TestDocTypeFromPath(t *testing.T) {
	res := DocTypeFromPath("/mirror/fcs_regions/Irkutskaja_obl/regulationrules/fcsRegulationRules_01343000654179867_105087.xml")
	if res != "fcsRegulationRules" {
		t.Error("res must be fcsRegulationRules, has: ", res)
	}

	println(res)

	res = DocTypeFromPath("/mirror/fcs_regions/Irkutskaja_obl/regulationrules/fcs_RegulationRules_01343000654179867_105087.xml")
	if res != "fcs_RegulationRules" {
		t.Error("res must be fcsRegulationRules, has: ", res)
	}
	println(res)
}
