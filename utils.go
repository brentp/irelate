package irelate

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/brentp/bix"
	"github.com/brentp/irelate/interfaces"
)

const MaxUint32 = ^uint32(0)
const MaxInt32 = int(MaxUint32 >> 1)

func RegionToParts(region string) (string, int, int, error) {
	parts := strings.Split(region, ":")
	// e.g. just "chr"
	if len(parts) == 1 {
		parts = append(parts, fmt.Sprintf("1-%d", MaxInt32))
	}

	se := strings.Split(parts[1], "-")
	if len(se) != 2 {
		return "", 0, 0, errors.New(fmt.Sprintf("unable to parse region: %s", region))
	}
	s, err := strconv.Atoi(se[0])
	if err != nil {
		return "", 0, 0, errors.New(fmt.Sprintf("unable to parse region: %s", region))
	}
	e, err := strconv.Atoi(se[1])
	if err != nil {
		return "", 0, 0, errors.New(fmt.Sprintf("unable to parse region: %s", region))
	}
	return parts[0], s, e, nil
}

func AsQueryable(f string) (interfaces.Queryable, error) {
	return bix.New(f, 1)
}
