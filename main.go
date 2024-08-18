package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/urfave/cli/v2"
	"log"
	"math/big"
	"os"
	"strconv"
)

type entry struct {
	BlockTime  uint64
	L1Root     []byte
	ParentHash []byte
}

func (e *entry) Marshal() []byte {
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

func (e *entry) Unmarshal(data []byte) {
	e.L1Root = data[:32]
	e.ParentHash = data[32:64]
	blkTime := data[64 : 64+8]
	_, err := binary.Decode(blkTime, binary.NativeEndian, &e.BlockTime)
	if err != nil {
		return
	}
}

var Client *ethclient.Client

func initEthclient() {
	client, err := ethclient.Dial("https://rpc.ankr.com/eth_sepolia")
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
				Usage:   "Scrape the blockchain",
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

	//var queryStartBlock *big.Int
	var startBlock uint64
	starterBlockArg := c.Args().First()
	if starterBlockArg == "" {
		//queryStartBlock = big.NewInt(int64(currentBlockNumber - 3000))
		startBlock = currentBlockNumber - 3000
	} else {
		rewindAmount, _ := strconv.ParseUint(starterBlockArg, 10, 64)
		startBlock = currentBlockNumber - rewindAmount
	}

	fmt.Println("starting block number:", startBlock)

	topic := common.BytesToHash(common.FromHex("0x3e54d0825ed78523037d00a81759237eb436ce774bd546993ee67a1b67b6e766"))

	var startB, endB *big.Int
	startB = big.NewInt(0).SetUint64(startBlock)
	endB = big.NewInt(0).SetUint64(startBlock + 3000)

	var counter uint64

	for {
		var done bool
		if endB.Cmp(currentBlockNumberBigint) == 1 {
			fmt.Println("ending run")
			endB = currentBlockNumberBigint
			if endB.Cmp(startB) == -1 {
				startB = endB
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

		logs, err := Client.FilterLogs(context.Background(), query)
		if err != nil {
			log.Fatal(err)
		}

		for _, l := range logs {
			//fmt.Println("Log Data:", l.Index, l.BlockNumber, common.Bytes2Hex(l.Data))

			block, err := Client.HeaderByNumber(context.Background(), big.NewInt(int64(l.BlockNumber)))
			if err != nil {
				log.Fatal(err)
			}
			//fmt.Println(block.Number)
			//fmt.Println("PH, Time:", block.ParentHash, block.Time)
			e := entry{
				L1Root:     l.Data, // the event only has the L1 root in the data field so we can just take it as-is
				BlockTime:  block.Time,
				ParentHash: block.ParentHash.Bytes(),
			}
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
		endB.Add(endB, big.NewInt(3000)) // move the window and repeat
		fmt.Println("Next batch...")
		//time.Sleep(1 * time.Second) // safe back-off
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

		var currEntry entry
		currEntry.Unmarshal(iter.Value())
		fmt.Println(index, currEntry.BlockTime, common.Bytes2Hex(currEntry.L1Root), common.Bytes2Hex(currEntry.ParentHash))
	}
	return nil
}
