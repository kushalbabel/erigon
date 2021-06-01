package ethdb

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/log"
)

func Get(tx KVGetter, bucket string, key []byte) ([]byte, error) {
	v, err := tx.GetOne(bucket, key)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

func ForEach(c Cursor, walker func(k, v []byte) (bool, error)) error {
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		ok, err := walker(k, v)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return nil
}

func Walk(c Cursor, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	k, v, err := c.Seek(startkey)
	if err != nil {
		return err
	}
	for k != nil && len(k) >= fixedbytes && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
		goOn, err := walker(k, v)
		if err != nil {
			return err
		}
		if !goOn {
			break
		}
		k, v, err = c.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func MultiPut(tx RwTx, tuples ...[]byte) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	count := 0
	total := float64(len(tuples)) / 3
	for bucketStart := 0; bucketStart < len(tuples); {
		bucketEnd := bucketStart
		for ; bucketEnd < len(tuples) && bytes.Equal(tuples[bucketEnd], tuples[bucketStart]); bucketEnd += 3 {
		}
		bucketName := string(tuples[bucketStart])
		c, err := tx.RwCursor(bucketName)
		if err != nil {
			return err
		}

		// move cursor to a first element in batch
		// if it's nil, it means all keys in batch gonna be inserted after end of bucket (batch is sorted and has no duplicates here)
		// can apply optimisations for this case
		firstKey, _, err := c.Seek(tuples[bucketStart+1])
		if err != nil {
			return err
		}
		isEndOfBucket := firstKey == nil

		l := (bucketEnd - bucketStart) / 3
		for i := 0; i < l; i++ {
			k := tuples[bucketStart+3*i+1]
			v := tuples[bucketStart+3*i+2]
			if isEndOfBucket {
				if v == nil {
					// nothing to delete after end of bucket
				} else {
					if err := c.Append(k, v); err != nil {
						return err
					}
				}
			} else {
				if v == nil {
					if err := c.Delete(k, nil); err != nil {
						return err
					}
				} else {
					if err := c.Put(k, v); err != nil {
						return err
					}
				}
			}

			count++

			select {
			default:
			case <-logEvery.C:
				progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, total/1_000_000)
				log.Info("Write to db", "progress", progress, "current table", bucketName)
			}
		}

		bucketStart = bucketEnd
	}
	return nil
}

func testKVPath() string {
	dir, err := ioutil.TempDir(os.TempDir(), "erigon-test-db")
	if err != nil {
		panic(err)
	}
	return dir
}

// todo: return TEVM code and use it
func GetCheckTEVM(db KVGetter) func(contractAddr common.Address, hash *common.Hash) (bool, error) {
	checked := map[common.Hash]struct{}{}
	var ok bool

	return func(contractAddr common.Address, codeHash *common.Hash) (bool, error) {
		if codeHash == nil {
			codeHashKey := make([]byte, common.HashLength+dbutils.NumberLength)
			hashedAddr := contractAddr[:]

			inc, err := Get(db, dbutils.IncarnationMapBucket, hashedAddr)
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				return false, fmt.Errorf("can't read code incarnation bucket by address %q: %w",
					contractAddr.String(), err)
			}

			addrHash, err := common.HashData(hashedAddr)
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				return false, fmt.Errorf("can't get address hash from address %q: %w",
					contractAddr.String(), err)
			}

			copy(codeHashKey[:common.HashLength], addrHash[:])
			copy(codeHashKey[common.HashLength:], inc)

			codeHashBytes, err := Get(db, dbutils.ContractCodeBucket, codeHashKey)
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				return false, fmt.Errorf("can't read code bucket by address %q: %w",
					contractAddr.String(), err)
			}

			h := common.BytesToHash(codeHashBytes)
			codeHash = &h
		}

		hash := *codeHash

		if _, ok = checked[hash]; ok {
			return true, nil
		}

		v, err := Get(db, dbutils.CallTraceSet, contractAddr.Bytes())
		if errors.Is(err, ErrKeyNotFound) {
			return false, nil
		} else if err != nil {
			return false, fmt.Errorf("can't get traces by contract address %q: %w",
				contractAddr.String(), err)
		}

		if v[common.AddressLength]&4 == 1 {
			// already scheduled to translation
			return true, nil
		}

		ok, err = db.Has(dbutils.ContractTEVMCodeBucket, hash.Bytes())
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return false, fmt.Errorf("can't check TEVM bucket by contract %q: %w",
				contractAddr.String(), err)
		}

		if !ok {
			checked[hash] = struct{}{}
		}

		return ok, nil
	}
}
