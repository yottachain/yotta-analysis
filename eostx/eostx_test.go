package eostx

import (
	"os"
	"testing"
)

var etx *EosTX

func TestMain(m *testing.M) {
	etx, _ = NewInstance("http://192.168.3.60:8888", "eosio", "5JutTDtj5BjY6o1wBnjo3eS78LHZjChJZCMDT4pqb6nKEBNbf1J", "hddpool12345", "hdddeposit12", "eosio")
	os.Exit(m.Run())
}

func TestGetFreezedLogs(t *testing.T) {
	p, err := etx.GetPledgeData(7)
	if err != nil {
		t.Log(err)
	}
	// event := m["0x549f6b834b7e9bf2c0d7fc414406eb8e51b900de568fc909a277d8a0d58e2c6f"]
	t.Logf("deposit: %d\n", p.Deposit.Amount)
}
