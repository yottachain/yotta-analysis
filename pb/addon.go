package pb

import (
	"bytes"
	"crypto"
)

// VerifyVHF 验证 DAT sha3 256 和vhf 是否相等
func (req *DownloadShardRequest) VerifyVHF(data []byte) bool {
	h := crypto.MD5.New()
	h.Write(data)
	return bytes.Equal(h.Sum(nil), req.VHF[:])
}
