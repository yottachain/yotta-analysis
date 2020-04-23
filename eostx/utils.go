package eostx

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	eos "github.com/eoscanada/eos-go"
)

func NewYTAAssetFromString(amount string) (out eos.Asset, err error) {
	if len(amount) == 0 {
		return out, fmt.Errorf("cannot be an empty string")
	}

	if strings.Contains(amount, " YTA") {
		amount = strings.Replace(amount, " YTA", "", 1)
	}
	if !strings.Contains(amount, ".") {
		val, err := strconv.ParseInt(amount, 10, 64)
		if err != nil {
			return out, err
		}
		return NewYTAAsset(val * 10000), nil
	}

	parts := strings.Split(amount, ".")
	if len(parts) != 2 {
		return out, fmt.Errorf("cannot have two . in amount")
	}

	if len(parts[1]) > 4 {
		return out, fmt.Errorf("YTA has only 4 decimals")
	}

	val, err := strconv.ParseInt(strings.Replace(amount, ".", "", 1), 10, 64)
	if err != nil {
		return out, err
	}
	return NewYTAAsset(val * int64(math.Pow10(4-len(parts[1])))), nil
}

func NewYTAAsset(amount int64) eos.Asset {
	return eos.Asset{Amount: eos.Int64(amount), Symbol: YTASymbol}
}
