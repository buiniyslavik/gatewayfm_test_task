package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/maps"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type Entry struct {
	BlockTime  uint64
	L1Root     []byte
	ParentHash []byte
}

type RPCRequest struct {
	Version string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type RPCResponse struct {
	Version string           `json:"jsonrpc"`
	ID      int              `json:"id"`
	Result  GetBlockResponse `json:"result"`
}

type GetBlockResponse struct {
	ParentHash string `json:"parentHash"`
	Timestamp  string `json:"timestamp"`
}

func (e *Entry) Marshal() []byte {
	result := []byte{}
	result = append(result, e.L1Root...)
	result = append(result, e.ParentHash...)
	timestamp := make([]byte, 8)
	_, err := binary.Encode(timestamp, binary.NativeEndian, e.BlockTime)
	if err != nil {
		return nil
	}
	result = append(result, timestamp...)
	return result
}

func (e *Entry) Unmarshal(data []byte) {
	e.L1Root = data[:32]
	e.ParentHash = data[32:64]
	blkTime := data[64 : 64+8]
	_, err := binary.Decode(blkTime, binary.NativeEndian, &e.BlockTime)
	if err != nil {
		return
	}
}

var Client *ethclient.Client

const RpcUrl = "https://rpc.ankr.com/eth_sepolia"
const WindowSize = 3000 // seems to work for this endpoint, may be increased if supported

func initEthclient() {
	client, err := ethclient.Dial(RpcUrl)
	if err != nil {
		log.Fatal(err)
	}
	Client = client
}

func main() {
	app := cli.App{
		Name:  "Gateway.fm Assignment",
		Usage: "Index certain Sepolia contract's events",
		Authors: []*cli.Author{
			{
				Name:  "Sviatoslav Osin",
				Email: "svosin@gmail.com",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "run",
				Aliases: []string{"r"},
				Usage:   "Scrape the blockchain. Pass the number of blocks to look back at, default = 3000",
				Action:  start,
			},
			{
				Name:    "dump",
				Aliases: []string{"d"},
				Usage:   "Print collected events",
				Action:  dump,
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func start(c *cli.Context) error {
	initEthclient()

	db, err := leveldb.OpenFile("events.ldb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	account := common.HexToAddress("0xA13Ddb14437A8F34897131367ad3ca78416d6bCa")

	currentBlockNumber, err := Client.BlockNumber(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	currentBlockNumberBigint := big.NewInt(0).SetUint64(currentBlockNumber)

	var startBlock uint64
	starterBlockArg := c.Args().First()
	if starterBlockArg == "" {
		startBlock = currentBlockNumber - WindowSize
	} else {
		rewindAmount, _ := strconv.ParseUint(starterBlockArg, 10, 64)
		startBlock = currentBlockNumber - rewindAmount
	}

	fmt.Println("starting block number:", startBlock)

	topic := common.BytesToHash(common.FromHex("0x3e54d0825ed78523037d00a81759237eb436ce774bd546993ee67a1b67b6e766"))

	var startB, endB *big.Int
	startB = big.NewInt(0).SetUint64(startBlock)
	endB = big.NewInt(0).SetUint64(startBlock + WindowSize)

	var counter uint64

	for {
		var done bool

		if endB.Cmp(currentBlockNumberBigint) == 1 {
			endB = currentBlockNumberBigint
			if endB.Cmp(startB) == -1 {
				break
			}
			done = true
		}
		fmt.Println("processing blocks", startB.String(), "-", endB.String())

		query := ethereum.FilterQuery{
			FromBlock: startB, //big.NewInt(6523000),
			ToBlock:   endB,
			Addresses: []common.Address{account},
			Topics:    [][]common.Hash{{topic}},
		}
		batchEntries := make(map[uint64]*Entry)
		logs, err := Client.FilterLogs(context.Background(), query)
		if err != nil {
			log.Fatal(err)
		}

		for _, l := range logs {
			batchEntries[l.BlockNumber] = &Entry{
				L1Root: l.Data,
			}
			// we have a map of half-baked entries, now we need to get the block headers
		}

		// we need to take a slice that is maps keys (block nums) and batch-feed it to the rpc
		blockNumSlice := maps.Keys(batchEntries)
		headerMap, err := HeaderByNumberBatch(blockNumSlice)
		if err != nil {
			panic(err)
		}

		for blockNum, hdr := range headerMap {
			var blkTime uint64
			blkTBytes := common.Hex2BytesFixed(hdr.Timestamp[2:], 8) // trim the 0x
			_, err = binary.Decode(blkTBytes, binary.LittleEndian, &blkTime)
			if err != nil {
				panic(err)
			}
			batchEntries[blockNum].BlockTime = blkTime
			batchEntries[blockNum].ParentHash = common.Hex2BytesFixed(hdr.ParentHash[2:], 32) // trim 0x as well

		}

		for _, e := range batchEntries {
			ctr := make([]byte, 8)
			_, err = binary.Encode(ctr, binary.NativeEndian, counter)
			if err != nil {
				log.Fatal(err)
			}
			err = db.Put(ctr, e.Marshal(), nil)
			if err != nil {
				log.Fatal(err)
			}
			counter++
		}

		if done {
			break
		}
		startB.Set(endB)
		startB.Add(startB, big.NewInt(1))
		endB.Add(endB, big.NewInt(WindowSize)) // move the window and repeat
	}

	fmt.Printf("processed %d entries\n", counter)
	return nil
}

func dump(c *cli.Context) error {
	db, err := leveldb.OpenFile("events.ldb", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		idx := iter.Key()

		index := binary.NativeEndian.Uint64(idx)

		var currEntry Entry
		currEntry.Unmarshal(iter.Value())
		fmt.Println(index, currEntry.BlockTime, common.Bytes2Hex(currEntry.L1Root), common.Bytes2Hex(currEntry.ParentHash))
	}
	return nil
}

// HeaderByNumberBatch returns a block header from the current canonical chain. If number is
// nil, the latest known header is returned.
func HeaderByNumberBatch(number []uint64) (map[uint64]*GetBlockResponse, error) {
	//batchElems := make([]rpc.BatchElem, len(number))
	// check for special case
	if len(number) == 2 && number[0] == number[1] {
		number = number[:0]
	}

	requests := []RPCRequest{}
	headers := make(map[uint64]*GetBlockResponse, len(number))
	for i, bn := range number {
		be := RPCRequest{
			Version: "2.0",
			ID:      i,
			Method:  "eth_getBlockByNumber",
			Params:  []interface{}{strconv.FormatUint(bn, 10), false},
		}

		requests = append(requests, be)
	}

	data, err := json.Marshal(requests)
	if err != nil {
		panic(err)
	}
	resp, err := http.Post(RpcUrl, "application/json", strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	re := make([]RPCResponse, len(number))
	err = json.Unmarshal(body, &re)
	if err != nil {
		log.Println(string(body))
		return nil, err
	}
	// todo sort replies by id, ascending
	for i, el := range re {
		headers[number[i]] = &el.Result
	}

	return headers, err
}
