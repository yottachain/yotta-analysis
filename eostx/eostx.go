package eostx

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	_ "github.com/eoscanada/eos-go/system"
	_ "github.com/eoscanada/eos-go/token"
	ytcrypto "github.com/yottachain/YTCrypto"
)

// NewInstance create a new eostx instance contans connect url, contract owner and it's private key
func NewInstance(url, bpAccount, privateKey, contractOwnerM, contractOwnerD, shadowAccount string) (*EosTX, error) {
	api := eos.New(url)
	keyBag := &eos.KeyBag{}
	err := keyBag.ImportPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("import private key: %s", err)
	}
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey, _ := ytcrypto.GetPublicKeyByPrivateKey(privateKey)
		pubkey, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey))
		return []ecc.PublicKey{pubkey}, nil
	})
	return &EosTX{API: api, BpAccount: bpAccount, ContractOwnerM: contractOwnerM, ContractOwnerD: contractOwnerD, ShadowAccount: shadowAccount, PrivateKey: privateKey}, nil
}

//ChangeEosURL change EOS URL to another one
func (eostx *EosTX) ChangeEosURL(eosURL string) {
	eostx.Lock()
	defer eostx.Unlock()
	api := eos.New(eosURL)
	keyBag := &eos.KeyBag{}
	keyBag.ImportPrivateKey(eostx.PrivateKey)
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey, _ := ytcrypto.GetPublicKeyByPrivateKey(eostx.PrivateKey)
		pubkey, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey))
		return []ecc.PublicKey{pubkey}, nil
	})
	eostx.API = api
}

// AddSpace call contract to add profit to a miner assigned by minerID
func (eostx *EosTX) AddSpace(owner string, minerID, space uint64) error {
	eostx.RLock()
	defer eostx.RUnlock()
	action := &eos.Action{
		Account: eos.AN(eostx.ContractOwnerM),
		Name:    eos.ActN("addmprofit"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.ShadowAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(Profit{Owner: eos.AN(owner), MinerID: minerID, Space: space, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}

//GetExchangeRate get exchange rate between YTA and storage space
func (eostx *EosTX) GetExchangeRate() (int32, error) {
	eostx.RLock()
	defer eostx.RUnlock()
	req := eos.GetTableRowsRequest{
		Code:  eostx.ContractOwnerD,
		Scope: eostx.ContractOwnerD,
		Table: "gdepositrate",
		Limit: 1,
		JSON:  true,
	}
	resp, err := eostx.API.GetTableRows(req)
	if err != nil {
		return 0, fmt.Errorf("get table row failed：get exchange rate")
	}
	if resp.More == true {
		return 0, fmt.Errorf("more than one rows returned：get exchange rate")
	}
	rows := make([]map[string]int32, 0)
	err = json.Unmarshal(resp.Rows, &rows)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("no row found：get exchange rate")
	}
	return rows[0]["rate"], nil
}

//GetPledgeData get pledge data of one miner
func (eostx *EosTX) GetPledgeData(minerid uint64) (*PledgeData, error) {
	eostx.RLock()
	defer eostx.RUnlock()
	req := eos.GetTableRowsRequest{
		Code:       eostx.ContractOwnerD,
		Scope:      eostx.ContractOwnerD,
		Table:      "miner2dep",
		LowerBound: fmt.Sprintf("%d", minerid),
		UpperBound: fmt.Sprintf("%d", minerid),
		Limit:      1,
		KeyType:    "i64",
		Index:      "1",
		JSON:       true,
	}
	resp, err := eostx.API.GetTableRows(req)
	if err != nil {
		return nil, fmt.Errorf("get table row failed, minerid: %d", minerid)
	}
	if resp.More == true {
		return nil, fmt.Errorf("more than one rows returned, minerid: %d", minerid)
	}
	rows := make([]PledgeData, 0)
	err = json.Unmarshal(resp.Rows, &rows)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no matched row found, minerid: %s", req.Scope)
	}
	return &rows[0], nil
}

// PayForfeit invalid miner need to pay forfeit
func (eostx *EosTX) DeducePledge(minerID uint64, count *eos.Asset) error {
	eostx.RLock()
	defer eostx.RUnlock()
	data, err := eostx.GetPledgeData(minerID)
	if err != nil {
		return err
	}
	err = eostx.payForfeit(data.AccountName, minerID, count)
	if err != nil {
		return err
	}
	// err = eostx.drawForfeit(data.AccountName)
	// if err != nil {
	// 	err = eostx.cutVote(data.AccountName)
	// 	return err
	// }
	return nil
}

// GetPoolInfoByPoolID fetch pool owner by pool ID
func (eostx *EosTX) GetPoolInfoByPoolID(poolID string) (*PoolInfo, error) {
	eostx.RLock()
	defer eostx.RUnlock()
	req := eos.GetTableRowsRequest{
		Code:       eostx.ContractOwnerM,
		Scope:      eostx.ContractOwnerM,
		Table:      "storepool",
		LowerBound: poolID,
		UpperBound: poolID,
		Limit:      1,
		KeyType:    "name",
		Index:      "1",
		JSON:       true,
	}
	resp, err := eostx.API.GetTableRows(req)
	if err != nil {
		return nil, fmt.Errorf("get table row failed, pool ID: %s", poolID)
	}
	if resp.More == true {
		return nil, fmt.Errorf("more than one rows returned, pool ID: %s", poolID)
	}
	rows := make([]PoolInfo, 0)
	err = json.Unmarshal(resp.Rows, &rows)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no matched row found, minerid: %s", req.Scope)
	}
	return &rows[0], nil
}

func (eostx *EosTX) payForfeit(user string, minerID uint64, count *eos.Asset) error {
	eostx.RLock()
	defer eostx.RUnlock()
	action := &eos.Action{
		Account: eos.AN(eostx.ContractOwnerD),
		Name:    eos.ActN("payforfeit"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.ShadowAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(PayForfeit{User: eos.AN(user), MinerID: minerID, Quant: *count, AccType: 2, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}

func (eostx *EosTX) drawForfeit(user string) error {
	eostx.RLock()
	defer eostx.RUnlock()
	action := &eos.Action{
		Account: eos.AN(eostx.ContractOwnerD),
		Name:    eos.ActN("drawforfeit"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.ShadowAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(DrawForfeit{User: eos.AN(user), AccType: 2, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}

func (eostx *EosTX) cutVote(user string) error {
	eostx.RLock()
	defer eostx.RUnlock()
	action := &eos.Action{
		Account: eos.AN(eostx.ContractOwnerD),
		Name:    eos.ActN("cutvote"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.ShadowAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(DrawForfeit{User: eos.AN(user), AccType: 2, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}

// CalculateProfit whether continuing calculating profit for a miner
func (eostx *EosTX) CalculateProfit(owner string, minerID uint64, flag bool) error {
	eostx.RLock()
	defer eostx.RUnlock()
	var actionName string
	if flag {
		actionName = "mactive"
	} else {
		actionName = "mdeactive"
	}
	action := &eos.Action{
		Account: eos.AN(eostx.ContractOwnerM),
		Name:    eos.ActN(actionName),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.ShadowAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(MActive{Owner: eos.AN(owner), MinerID: minerID, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}

//PreRegisterTrx extract all neccessary parameters from transaction
func (eostx *EosTX) PreRegisterTrx(trx string) (*eos.SignedTransaction, *RegMiner, error) {
	if trx == "" {
		return nil, nil, errors.New("input transaction can not be null")
	}
	var packedTrx *eos.PackedTransaction
	err := json.Unmarshal([]byte(trx), &packedTrx)
	if err != nil {
		return nil, nil, err
	}
	signedTrx, err := packedTrx.Unpack()
	if err != nil {
		return nil, nil, err
	}
	if len(signedTrx.Actions) != 1 {
		return nil, nil, errors.New("need at least one action")
	}
	var actionBytes []byte
	if signedTrx.Actions[0].ActionData.Data != nil {
		actionBytes, err = hex.DecodeString(string([]byte(signedTrx.Actions[0].ActionData.Data.(string))))
		if err != nil {
			return nil, nil, err
		}
	} else {
		actionBytes = []byte(signedTrx.Actions[0].ActionData.HexData)
	}
	decoder := eos.NewDecoder(actionBytes)
	data := new(RegMiner)
	err = decoder.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return signedTrx, data, nil
}

//ChangMinerPoolTrx extract all neccessary parameters from transaction
func (eostx *EosTX) ChangeMinerPoolTrx(trx string) (*eos.SignedTransaction, *ChangeMinerPool, error) {
	if trx == "" {
		return nil, nil, errors.New("input transaction can not be null")
	}
	var packedTrx *eos.PackedTransaction
	err := json.Unmarshal([]byte(trx), &packedTrx)
	if err != nil {
		return nil, nil, err
	}
	signedTrx, err := packedTrx.Unpack()
	var actionBytes []byte
	if signedTrx.Actions[0].ActionData.Data != nil {
		actionBytes, err = hex.DecodeString(string([]byte(signedTrx.Actions[0].ActionData.Data.(string))))
		if err != nil {
			return nil, nil, err
		}
	} else {
		actionBytes = []byte(signedTrx.Actions[0].ActionData.HexData)
	}
	decoder := eos.NewDecoder(actionBytes)
	data := new(ChangeMinerPool)
	err = decoder.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return signedTrx, data, nil
}

//ChangeAdminAccTrx extract all neccessary parameters from mchgadminacc transaction
func (eostx *EosTX) ChangeAdminAccTrx(trx string) (*eos.SignedTransaction, *ChangeAdminAcc, error) {
	if trx == "" {
		return nil, nil, errors.New("input transaction can not be null")
	}
	var packedTrx *eos.PackedTransaction
	err := json.Unmarshal([]byte(trx), &packedTrx)
	if err != nil {
		return nil, nil, err
	}
	signedTrx, err := packedTrx.Unpack()
	var actionBytes []byte
	if signedTrx.Actions[0].ActionData.Data != nil {
		actionBytes, err = hex.DecodeString(string([]byte(signedTrx.Actions[0].ActionData.Data.(string))))
		if err != nil {
			return nil, nil, err
		}
	} else {
		actionBytes = []byte(signedTrx.Actions[0].ActionData.HexData)
	}
	decoder := eos.NewDecoder(actionBytes)
	data := new(ChangeAdminAcc)
	err = decoder.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return signedTrx, data, nil
}

//ChangeProfitAccTrx extract all neccessary parameters from mchgowneracc transaction
func (eostx *EosTX) ChangeProfitAccTrx(trx string) (*eos.SignedTransaction, *ChangeProfitAcc, error) {
	if trx == "" {
		return nil, nil, errors.New("input transaction can not be null")
	}
	var packedTrx *eos.PackedTransaction
	err := json.Unmarshal([]byte(trx), &packedTrx)
	if err != nil {
		return nil, nil, err
	}
	signedTrx, err := packedTrx.Unpack()
	var actionBytes []byte
	if signedTrx.Actions[0].ActionData.Data != nil {
		actionBytes, err = hex.DecodeString(string([]byte(signedTrx.Actions[0].ActionData.Data.(string))))
		if err != nil {
			return nil, nil, err
		}
	} else {
		actionBytes = []byte(signedTrx.Actions[0].ActionData.HexData)
	}
	decoder := eos.NewDecoder(actionBytes)
	data := new(ChangeProfitAcc)
	err = decoder.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return signedTrx, data, nil
}

//ChangePoolIDTrx extract all neccessary parameters from mchgstrpool transaction
func (eostx *EosTX) ChangePoolIDTrx(trx string) (*eos.SignedTransaction, *ChangePoolID, error) {
	if trx == "" {
		return nil, nil, errors.New("input transaction can not be null")
	}
	var packedTrx *eos.PackedTransaction
	err := json.Unmarshal([]byte(trx), &packedTrx)
	if err != nil {
		return nil, nil, err
	}
	signedTrx, err := packedTrx.Unpack()
	if err != nil {
		return nil, nil, err
	}
	var actionBytes []byte
	if signedTrx.Actions[0].ActionData.Data != nil {
		actionBytes, err = hex.DecodeString(string([]byte(signedTrx.Actions[0].ActionData.Data.(string))))
		if err != nil {
			return nil, nil, err
		}
	} else {
		actionBytes = []byte(signedTrx.Actions[0].ActionData.HexData)
	}
	decoder := eos.NewDecoder(actionBytes)
	data := new(ChangePoolID)
	err = decoder.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return signedTrx, data, nil
}

//ChangeDepAccTrx extract all neccessary parameters from mchgdepacc transaction
func (eostx *EosTX) ChangeDepAccTrx(trx string) (*eos.SignedTransaction, *ChangeDepAcc, error) {
	if trx == "" {
		return nil, nil, errors.New("input transaction can not be null")
	}
	var packedTrx *eos.PackedTransaction
	err := json.Unmarshal([]byte(trx), &packedTrx)
	if err != nil {
		return nil, nil, err
	}
	signedTrx, err := packedTrx.Unpack()
	if err != nil {
		return nil, nil, err
	}
	var actionBytes []byte
	if signedTrx.Actions[0].ActionData.Data != nil {
		actionBytes, err = hex.DecodeString(string([]byte(signedTrx.Actions[0].ActionData.Data.(string))))
		if err != nil {
			return nil, nil, err
		}
	} else {
		actionBytes = []byte(signedTrx.Actions[0].ActionData.HexData)
	}
	decoder := eos.NewDecoder(actionBytes)
	data := new(ChangeDepAcc)
	err = decoder.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return signedTrx, data, nil
}

//ChangeDepositTrx change deposit of miner
func (eostx *EosTX) ChangeDepositTrx(trx string) (*eos.SignedTransaction, *ChangeDeposit, error) {
	if trx == "" {
		return nil, nil, errors.New("input transaction can not be null")
	}
	var packedTrx *eos.PackedTransaction
	err := json.Unmarshal([]byte(trx), &packedTrx)
	if err != nil {
		return nil, nil, err
	}
	signedTrx, err := packedTrx.Unpack()
	if err != nil {
		return nil, nil, err
	}
	var actionBytes []byte
	if signedTrx.Actions[0].ActionData.Data != nil {
		actionBytes, err = hex.DecodeString(string([]byte(signedTrx.Actions[0].ActionData.Data.(string))))
		if err != nil {
			return nil, nil, err
		}
	} else {
		actionBytes = []byte(signedTrx.Actions[0].ActionData.HexData)
	}
	decoder := eos.NewDecoder(actionBytes)
	data := new(ChangeDeposit)
	err = decoder.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return signedTrx, data, nil
}

//ChangeAssignedSpaceTrx extract all neccessary parameters from mchgspace transaction
func (eostx *EosTX) ChangeAssignedSpaceTrx(trx string) (*eos.SignedTransaction, *ChangeAssignedSpace, error) {
	if trx == "" {
		return nil, nil, errors.New("input transaction can not be null")
	}
	var packedTrx *eos.PackedTransaction
	err := json.Unmarshal([]byte(trx), &packedTrx)
	if err != nil {
		return nil, nil, err
	}
	signedTrx, err := packedTrx.Unpack()
	var actionBytes []byte
	if signedTrx.Actions[0].ActionData.Data != nil {
		actionBytes, err = hex.DecodeString(string([]byte(signedTrx.Actions[0].ActionData.Data.(string))))
		if err != nil {
			return nil, nil, err
		}
	} else {
		actionBytes = []byte(signedTrx.Actions[0].ActionData.HexData)
	}
	decoder := eos.NewDecoder(actionBytes)
	data := new(ChangeAssignedSpace)
	err = decoder.Decode(data)
	if err != nil {
		return nil, nil, err
	}
	return signedTrx, data, nil
}

func (eostx *EosTX) SendTrx(signedTx *eos.SignedTransaction) error {
	eostx.RLock()
	defer eostx.RUnlock()
	packedTx, err := signedTx.Pack(eos.CompressionNone)
	if err != nil {
		return err
	}
	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}
