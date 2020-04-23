package eostx

import (
	"encoding/json"
	"fmt"
	"sync"

	eos "github.com/eoscanada/eos-go"
)

type FlexString string

func (fi *FlexString) UnmarshalJSON(b []byte) error {
	if b[0] != '"' {
		var intvalue int64
		err := json.Unmarshal(b, &intvalue)
		if err != nil {
			return err
		}
		*fi = FlexString(fmt.Sprintf("%d", intvalue))
		return nil
	} else {
		*fi = FlexString(string(b[1 : len(b)-1]))
		return nil
	}
}

type EosTX struct {
	API            *eos.API
	BpAccount      string
	ContractOwnerM string
	ContractOwnerD string
	ShadowAccount  string
	PrivateKey     string
	sync.RWMutex
}

type RegMiner struct {
	MinerID   uint64          `json:"minerid"`
	Owner     eos.AccountName `json:"adminacc"`
	DepAcc    eos.AccountName `json:"dep_acc"`
	DepAmount eos.Asset       `json:"dep_amoun"`
	Extra     string          `json:"extra"`
}

type ChangeMinerPool struct {
	MinerID     uint64          `json:"minerid"`
	PoolID      eos.AccountName `json:"pool_id"`
	MinerProfit eos.AccountName `json:"minerowner"`
	MaxSpace    uint64          `json:"max_space"`
}

type Miner struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Caller  eos.AccountName `json:"caller"`
}

type Profit struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Space   uint64          `json:"space"`
	Caller  eos.AccountName `json:"caller"`
}

type PledgeData struct {
	MinerID     uint32    `json:"minerid"`
	AccountName string    `json:"account_name"`
	Deposit     eos.Asset `json:"deposit"`
	Total       eos.Asset `json:"dep_total"`
}

type PoolInfo struct {
	Owner     eos.AccountName `json:"pool_owner"`
	PoolID    eos.AccountName `json:"pool_id"`
	MaxSpace  FlexString      `json:"max_space"`
	SpaceLeft FlexString      `json:"space_left"`
}

type PayForfeit struct {
	User    eos.AccountName `json:"user"`
	MinerID uint64          `json:"minerid"`
	Quant   eos.Asset       `json:"quant"`
	AccType uint8           `json:"acc_type"`
	Caller  eos.AccountName `json:"caller"`
}

type DrawForfeit struct {
	User    eos.AccountName `json:"user"`
	AccType uint8           `json:"acc_type"`
	Caller  eos.AccountName `json:"caller"`
}

type MActive struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Caller  eos.AccountName `json:"caller"`
}

type ChangeAdminAcc struct {
	MinerID     uint64          `json:"minerid"`
	NewAdminAcc eos.AccountName `json:"new_adminacc"`
}

type ChangeProfitAcc struct {
	MinerID      uint64          `json:"minerid"`
	NewProfitAcc eos.AccountName `json:"new_owneracc"`
}

type ChangePoolID struct {
	MinerID   uint64          `json:"minerid"`
	NewPoolID eos.AccountName `json:"new_poolid"`
}

type ChangeDepAcc struct {
	MinerID   uint64          `json:"minerid"`
	NewDepAcc eos.AccountName `json:"new_depacc"`
}

type ChangeDeposit struct {
	User       eos.AccountName `json:"user"`
	MinerID    uint64          `json:"minerid"`
	IsIncrease bool            `json:"is_increase"`
	Quant      eos.Asset       `json:"quant"`
}

type ChangeAssignedSpace struct {
	MinerID  uint64 `json:"minerid"`
	MaxSpace uint64 `json:"max_space"`
}

// YTASymbol represents the standard YTA symbol on the chain.
var YTASymbol = eos.Symbol{Precision: 4, Symbol: "YTA"}
