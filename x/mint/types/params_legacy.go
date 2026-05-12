/*
NOTE: Usage of x/params to manage parameters is deprecated in favor of x/gov
controlled execution of MsgUpdateParams messages. These types remain solely
for migration purposes and will be removed in a future release.
*/
package types

import (
	"fmt"

	sdkmath "cosmossdk.io/math"

	paramtypes "github.com/cosmos/cosmos-sdk/x/params/types"
)

// Parameter store keys
var (
	KeyMintDenom           = []byte("MintDenom")
	KeyInflationRateChange = []byte("InflationRateChange")
	KeyInflationMax        = []byte("InflationMax")
	KeyInflationMin        = []byte("InflationMin")
	KeyGoalBonded          = []byte("GoalBonded")
	KeyBlocksPerYear       = []byte("BlocksPerYear")
)

// Deprecated: ParamKeyTable for the mint module. Retained for downstream
// chains that still register a legacy x/params subspace for migration paths.
func ParamKeyTable() paramtypes.KeyTable {
	return paramtypes.NewKeyTable().RegisterParamSet(&Params{})
}

// Deprecated: ParamSetPairs implements params.ParamSet. Validators here adapt
// the new typed validator signatures back to the legacy any-flavored form.
func (p *Params) ParamSetPairs() paramtypes.ParamSetPairs {
	return paramtypes.ParamSetPairs{
		paramtypes.NewParamSetPair(KeyMintDenom, &p.MintDenom, legacyValidate(validateMintDenom)),
		paramtypes.NewParamSetPair(KeyInflationRateChange, &p.InflationRateChange, legacyValidate(validateInflationRateChange)),
		paramtypes.NewParamSetPair(KeyInflationMax, &p.InflationMax, legacyValidate(validateInflationMax)),
		paramtypes.NewParamSetPair(KeyInflationMin, &p.InflationMin, legacyValidate(validateInflationMin)),
		paramtypes.NewParamSetPair(KeyGoalBonded, &p.GoalBonded, legacyValidate(validateGoalBonded)),
		paramtypes.NewParamSetPair(KeyBlocksPerYear, &p.BlocksPerYear, legacyValidate(validateBlocksPerYear)),
	}
}

func legacyValidate[T any](fn func(T) error) func(any) error {
	return func(i any) error {
		v, ok := i.(T)
		if !ok {
			var zero T
			return fmt.Errorf("invalid parameter type: %T (expected %T)", i, zero)
		}
		return fn(v)
	}
}

// keep the import live without forcing callers to import sdkmath directly
var _ = sdkmath.LegacyDec{}
